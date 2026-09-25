package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.codec.EncodeException;
import com.github.h3yow.robottopicdeliveryjava.codec.MessageCodec;
import com.github.h3yow.robottopicdeliveryjava.config.BrokerConfig;
import com.github.h3yow.robottopicdeliveryjava.model.Message;
import com.github.h3yow.robottopicdeliveryjava.topic.InvalidTopicException;
import com.github.h3yow.robottopicdeliveryjava.topic.Topic;
import com.github.h3yow.robottopicdeliveryjava.topic.TopicFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 进程内消息 broker。
 *
 * 语义要点：
 * - 每个订阅一个独立有界队列与单派发线程：同一订阅内严格按序投递，重试会阻塞后续消息（队头阻塞语义）。
 * - 队列满时 publish 对该订阅返回 QUEUE_FULL 背压拒绝，不影响其他订阅。
 * - 投递等待确认直至超时；超时后重试，重试耗尽进入 DEAD 终态。
 * - ack 幂等：重复确认计 duplicateAcks，迟到确认（已 ACKED 之后按旧 attempt、或已 DEAD/已移除）计 lateAcks，均不改变终态。
 * - disconnect 暂停派发并保留全部未确认消息；reconnect 后补投，不静默丢失。
 */
public class MessageBroker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageBroker.class);

    private final BrokerConfig config;
    private final MessageCodec codec;
    private final BrokerStats stats = new BrokerStats();
    private final Map<String, SubscriptionContext> subscriptions = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public MessageBroker(BrokerConfig config, MessageCodec codec) {
        this.config = config;
        this.codec = codec;
    }

    // ---------------------------------------------------------------- 订阅管理

    public Subscription subscribe(TopicFilter filter, MessageListener listener) {
        String id = UUID.randomUUID().toString();
        SubscriptionContext ctx = new SubscriptionContext(id, filter, listener);
        subscriptions.put(id, ctx);
        ctx.worker.start();
        log.info("[subscribe] id={} filter={} queueCapacity={}", id, filter.pattern(),
                config.queueCapacityPerSubscription());
        return new Subscription(id, filter);
    }

    public void unsubscribe(String subscriptionId) {
        SubscriptionContext ctx = subscriptions.remove(subscriptionId);
        if (ctx == null) {
            log.warn("[unsubscribe] unknown subscription id={}", subscriptionId);
            return;
        }
        ctx.removed = true;
        synchronized (ctx.lifecycleLock) {
            ctx.lifecycleLock.notifyAll();
        }
        // 排空队列：未派发的消息计为 dropped（确定性的移除语义）
        int drained = ctx.drainQueue();
        // 在途未确认的消息也计为 dropped
        if (ctx.current != null) {
            synchronized (ctx.current) {
                if (ctx.current.state == DeliveryState.IN_FLIGHT || ctx.current.state == DeliveryState.PENDING) {
                    ctx.current.state = DeliveryState.DEAD;
                    ctx.current.notifyAll();
                    ctx.deliveries.remove(ctx.current.message.id());
                    drained++;
                }
            }
        }
        if (drained > 0) {
            for (int i = 0; i < drained; i++) {
                stats.incDropped();
            }
        }
        ctx.worker.interrupt();
        log.info("[unsubscribe] id={} droppedInFlight={}", subscriptionId, drained);
    }

    // ---------------------------------------------------------------- 发布

    public PublishReceipt publish(String topicName, Object payload) {
        if (closed) {
            stats.incPublishRejected();
            log.warn("[publish] rejected reason={} topic={}", RejectReason.BROKER_CLOSED, topicName);
            return PublishReceipt.rejected(RejectReason.BROKER_CLOSED);
        }
        final Topic topic;
        try {
            topic = Topic.of(topicName);
        } catch (InvalidTopicException e) {
            stats.incPublishRejected();
            log.warn("[publish] rejected reason={} topic={} detail={}",
                    RejectReason.INVALID_TOPIC, topicName, e.getMessage());
            return PublishReceipt.rejected(RejectReason.INVALID_TOPIC);
        }
        if (payload == null) {
            stats.incPublishRejected();
            log.warn("[publish] rejected reason={} topic={}", RejectReason.INVALID_PAYLOAD, topicName);
            return PublishReceipt.rejected(RejectReason.INVALID_PAYLOAD);
        }
        final byte[] encoded;
        try {
            encoded = codec.encode(payload);
        } catch (EncodeException e) {
            stats.incPublishRejected();
            log.warn("[publish] rejected reason={} topic={} detail={}",
                    RejectReason.ENCODE_FAILURE, topicName, e.getMessage());
            return PublishReceipt.rejected(RejectReason.ENCODE_FAILURE);
        }

        Message message = Message.of(topic, encoded);
        stats.incPublished();

        List<PublishReceipt.EnqueueResult> results = new ArrayList<>();
        for (SubscriptionContext ctx : subscriptions.values()) {
            if (ctx.removed || !ctx.filter.matches(topic)) {
                continue;
            }
            DeliveryTask task = new DeliveryTask(message);
            ctx.deliveries.put(message.id(), task);
            if (ctx.queue.offer(task)) {
                stats.incEnqueued();
                results.add(new PublishReceipt.EnqueueResult(ctx.id, true));
                log.debug("[enqueue] sub={} msg={} queueDepth={}", ctx.id, message.id(), ctx.queue.size());
            } else {
                ctx.deliveries.remove(message.id());
                stats.incEnqueueRejected();
                results.add(new PublishReceipt.EnqueueResult(ctx.id, false));
                log.warn("[backpressure] sub={} msg={} reason={} queueDepth={}",
                        ctx.id, message.id(), RejectReason.QUEUE_FULL, ctx.queue.size());
            }
        }
        log.info("[publish] accepted msg={} topic={} matchedSubs={}",
                message.id(), topicName, results.size());
        return PublishReceipt.accepted(message.id(), results);
    }

    // ---------------------------------------------------------------- 确认

    private void ack(String subscriptionId, String messageId, int attemptSeen) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx == null) {
            stats.incLateAcks();
            log.warn("[ack] late reason=subscription-removed sub={} msg={}", subscriptionId, messageId);
            return;
        }
        DeliveryTask task = ctx.deliveries.get(messageId);
        if (task == null) {
            stats.incLateAcks();
            log.warn("[ack] late reason=delivery-gone sub={} msg={}", subscriptionId, messageId);
            return;
        }
        synchronized (task) {
            switch (task.state) {
                case IN_FLIGHT -> {
                    if (attemptSeen == task.attempt) {
                        task.state = DeliveryState.ACKED; // 终态保留在状态表中，用于区分重复/迟到确认
                        stats.incAcked();
                        log.info("[ack] ok sub={} msg={} attempt={}", subscriptionId, messageId, attemptSeen);
                    } else {
                        // 旧 attempt 的迟到确认（乱序到达）：不改变当前状态
                        stats.incLateAcks();
                        log.warn("[ack] late reason=stale-attempt sub={} msg={} ackAttempt={} currentAttempt={}",
                                subscriptionId, messageId, attemptSeen, task.attempt);
                    }
                    task.notifyAll();
                }
                case ACKED -> {
                    stats.incDuplicateAcks();
                    log.warn("[ack] duplicate sub={} msg={} attemptSeen={}", subscriptionId, messageId, attemptSeen);
                }
                case DEAD -> {
                    stats.incLateAcks();
                    log.warn("[ack] late reason=dead sub={} msg={}", subscriptionId, messageId);
                }
                default -> {
                    stats.incLateAcks();
                    log.warn("[ack] late reason=unexpected-state sub={} msg={} state={}",
                            subscriptionId, messageId, task.state);
                }
            }
        }
    }

    // ---------------------------------------------------------------- 连接中断与恢复

    public void disconnect(String subscriptionId) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx == null) {
            log.warn("[disconnect] unknown subscription id={}", subscriptionId);
            return;
        }
        ctx.connected = false;
        log.info("[disconnect] sub={} queueDepth={} inFlight={}",
                subscriptionId, ctx.queue.size(), ctx.current != null ? 1 : 0);
    }

    public void reconnect(String subscriptionId) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx == null) {
            log.warn("[reconnect] unknown subscription id={}", subscriptionId);
            return;
        }
        long unacked = ctx.queue.size() + (ctx.current != null ? 1 : 0);
        ctx.connected = true;
        synchronized (ctx.lifecycleLock) {
            ctx.lifecycleLock.notifyAll();
        }
        stats.addRedeliveredOnReconnect(unacked);
        log.info("[reconnect] sub={} promisedRedelivery={}", subscriptionId, unacked);
    }

    // ---------------------------------------------------------------- 统计与诊断

    public BrokerStats stats() {
        return stats;
    }

    public Map<String, Map<String, Long>> diagnostics() {
        Map<String, Map<String, Long>> out = new ConcurrentHashMap<>();
        for (SubscriptionContext ctx : subscriptions.values()) {
            Map<String, Long> m = new ConcurrentHashMap<>();
            m.put("queueDepth", (long) ctx.queue.size());
            m.put("inFlight", ctx.current != null ? 1L : 0L);
            long pending = 0, inFlight = 0, acked = 0, dead = 0;
            for (DeliveryTask t : ctx.deliveries.values()) {
                switch (t.state) {
                    case PENDING -> pending++;
                    case IN_FLIGHT -> inFlight++;
                    case ACKED -> acked++;
                    case DEAD -> dead++;
                }
            }
            m.put("statePending", pending);
            m.put("stateInFlight", inFlight);
            m.put("stateAcked", acked);
            m.put("stateDead", dead);
            m.put("connected", ctx.connected ? 1L : 0L);
            out.put(ctx.id, m);
        }
        return out;
    }

    @Override
    public void close() {
        closed = true;
        for (String id : List.copyOf(subscriptions.keySet())) {
            unsubscribe(id);
        }
        log.info("[close] broker closed, final stats={}", stats.snapshot());
    }

    // ---------------------------------------------------------------- 内部结构

    private static final class DeliveryTask {
        final Message message;
        DeliveryState state = DeliveryState.PENDING;
        int attempt = 0;

        DeliveryTask(Message message) {
            this.message = message;
        }
    }

    private final class SubscriptionContext {
        final String id;
        final TopicFilter filter;
        final MessageListener listener;
        final LinkedBlockingDeque<DeliveryTask> queue =
                new LinkedBlockingDeque<>(config.queueCapacityPerSubscription());
        final Map<String, DeliveryTask> deliveries = new ConcurrentHashMap<>();
        final Object lifecycleLock = new Object();
        volatile boolean connected = true;
        volatile boolean removed = false;
        volatile DeliveryTask current;
        final Thread worker;

        SubscriptionContext(String id, TopicFilter filter, MessageListener listener) {
            this.id = id;
            this.filter = filter;
            this.listener = listener;
            this.worker = new Thread(this::dispatchLoop, "broker-dispatch-" + id.substring(0, 8));
            this.worker.setDaemon(true);
        }

        int drainQueue() {
            int n = 0;
            DeliveryTask t;
            while ((t = queue.poll()) != null) {
                deliveries.remove(t.message.id());
                n++;
            }
            return n;
        }

        void dispatchLoop() {
            try {
                while (!removed && !closed) {
                    DeliveryTask task = queue.poll(config.dispatchIntervalMillis(), TimeUnit.MILLISECONDS);
                    if (task == null) {
                        continue;
                    }
                    current = task;
                    try {
                        deliverUntilTerminal(task);
                    } finally {
                        current = null;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("[worker-exit] sub={}", id);
        }

        private void awaitConnected() throws InterruptedException {
            synchronized (lifecycleLock) {
                while (!connected && !removed && !closed) {
                    lifecycleLock.wait(50);
                }
            }
        }

        /** 对单条消息执行 投递->等待确认->超时重试 直至 ACKED 或 DEAD。 */
        private void deliverUntilTerminal(DeliveryTask task) throws InterruptedException {
            while (true) {
                synchronized (task) {
                    if (task.state == DeliveryState.ACKED || task.state == DeliveryState.DEAD) {
                        return;
                    }
                }
                awaitConnected();
                if (removed || closed) {
                    return;
                }
                final int thisAttempt;
                synchronized (task) {
                    task.attempt++;
                    task.state = DeliveryState.IN_FLIGHT;
                    thisAttempt = task.attempt;
                }
                stats.incDelivered();
                log.info("[deliver] sub={} msg={} attempt={} topic={}",
                        id, task.message.id(), thisAttempt, task.message.topic().name());

                Delivery delivery = new DeliveryImpl(id, task.message, thisAttempt);
                try {
                    listener.onMessage(delivery);
                } catch (Exception e) {
                    log.warn("[listener-error] sub={} msg={} attempt={} error={}",
                            id, task.message.id(), thisAttempt, e.toString());
                }

                // 等待确认直至超时
                long deadline = System.currentTimeMillis() + config.ackTimeoutMillis();
                synchronized (task) {
                    while (task.state == DeliveryState.IN_FLIGHT) {
                        long remain = deadline - System.currentTimeMillis();
                        if (remain <= 0) {
                            break;
                        }
                        task.wait(Math.min(remain, 50));
                    }
                    if (task.state == DeliveryState.ACKED) {
                        return;
                    }
                    if (task.state == DeliveryState.DEAD) {
                        return; // 被 unsubscribe 终止
                    }
                    // 超时
                    stats.incTimeouts();
                    if (thisAttempt > config.maxRetries()) {
                        task.state = DeliveryState.DEAD; // 终态保留在状态表中
                        stats.incDead();
                        log.warn("[dead] sub={} msg={} attempts={} reason=retries-exhausted",
                                id, task.message.id(), thisAttempt);
                        return;
                    }
                    task.state = DeliveryState.PENDING;
                    stats.incRetries();
                    log.info("[retry] sub={} msg={} nextAttempt={} reason=ack-timeout",
                            id, task.message.id(), thisAttempt + 1);
                }
            }
        }
    }

    private final class DeliveryImpl implements Delivery {
        private final String subscriptionId;
        private final Message message;
        private final int attempt;

        DeliveryImpl(String subscriptionId, Message message, int attempt) {
            this.subscriptionId = subscriptionId;
            this.message = message;
            this.attempt = attempt;
        }

        @Override
        public Message message() {
            return message;
        }

        @Override
        public int attempt() {
            return attempt;
        }

        @Override
        public void ack() {
            MessageBroker.this.ack(subscriptionId, message.id(), attempt);
        }
    }
}
