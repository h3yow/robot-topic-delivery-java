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

import java.time.Instant;
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
 * - 投递等待确认直至超时；超时后先进入 BACKOFF 退避等待（可配置逐次变长，线程等待不空转），
 *   退避结束才重试；退避期间断连不改变结果，恢复后只补投一次，不丢不重。
 * - 重试耗尽进入 DEAD 终态，并把消息完整留存到 {@link DeadLetterStore}（主题/负载/订阅/投递次数/
 *   终结时间），可列出、可重投。重投生成全新消息 id 走完整投递流程，旧记录标记 REDRIVEN 不复活、
 *   不重复生效。
 * - ack 幂等：重复确认计 duplicateAcks，迟到确认（已 ACKED 之后按旧 attempt、已 DEAD、退避中或已移除）
 *   计 lateAcks，均不改变终态。
 * - disconnect 暂停派发并保留全部未确认消息；reconnect 后补投，不静默丢失。
 */
public class MessageBroker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageBroker.class);

    private final BrokerConfig config;
    private final MessageCodec codec;
    private final BrokerStats stats = new BrokerStats();
    private final Map<String, SubscriptionContext> subscriptions = new ConcurrentHashMap<>();
    private final DeadLetterStore deadLetters;
    private volatile boolean closed = false;

    public MessageBroker(BrokerConfig config, MessageCodec codec) {
        this.config = config;
        this.codec = codec;
        this.deadLetters = new DeadLetterStore(
                config.deadLetterCapacity(), config.deadLetterOverflowPolicy());
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
        // 在途/退避中未确认的消息也确定地计为 dropped（不进入死信留存——留存只针对重试耗尽）
        // current 可能在取下后被派发线程置空，先快照到局部变量再使用，避免竞态 NPE
        DeliveryTask inFlight = ctx.current;
        if (inFlight != null) {
            synchronized (inFlight) {
                DeliveryState st = inFlight.state;
                if (st == DeliveryState.IN_FLIGHT || st == DeliveryState.PENDING
                        || st == DeliveryState.BACKOFF) {
                    inFlight.state = DeliveryState.DEAD;
                    inFlight.notifyAll();
                    ctx.deliveries.remove(inFlight.message.id());
                    drained++;
                }
            }
        }
        for (int i = 0; i < drained; i++) {
            stats.incDropped();
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

    // ---------------------------------------------------------------- 死信留存与重投

    /** 列出全部留存死信（含已重投的，按终结时间从旧到新）。 */
    public List<DeadMessage> listDeadMessages() {
        return deadLetters.list();
    }

    /** 列出某个订阅的留存死信。 */
    public List<DeadMessage> listDeadMessages(String subscriptionId) {
        return deadLetters.listForSubscription(subscriptionId);
    }

    /** 按留存记录 id 取单条（挑一条查看/重投时使用）。 */
    public DeadMessage deadMessage(String recordId) {
        return deadLetters.get(recordId);
    }

    /**
     * 将一条重试耗尽后留存的死信，作为一次全新投递重新投递给原订阅。
     * 重投产生新的消息 id，重新走确认/超时/退避/重试/终止的完整流程；原留存记录标记为
     * REDRIVEN，不会被复活，同一原始消息不会因此重复生效。
     *
     * 挡下原因彼此可分：
     * - SUBSCRIPTION_GONE：目标订阅不存在；
     * - STILL_IN_FLIGHT：原始消息仍在投递路上（未终结）；
     * - RECORD_NOT_FOUND：该订阅下无此留存记录（含已被容量淘汰、从未终止）；
     * - ALREADY_REDELIVERED：该记录已经重投过；
     * - QUEUE_FULL：重投那一刻目标订阅队列已满，沿用确定性背压拒绝，记录保留可再试；
     * - BROKER_CLOSED：broker 已关闭。
     */
    public RedeliveryResult redeliverDeadMessage(String subscriptionId, String originalMessageId) {
        stats.incRedriveRequested();
        if (closed) {
            return rejectRedrive(null, subscriptionId, originalMessageId,
                    RedeliveryRejectReason.BROKER_CLOSED);
        }
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx == null) {
            return rejectRedrive(null, subscriptionId, originalMessageId,
                    RedeliveryRejectReason.SUBSCRIPTION_GONE);
        }

        // 原始消息仍在投递路上（队列/在途/退避）：不属于可重投的留存消息。
        // 已 ACKED 的终态任务会保留在状态表中（用于区分重复确认），它不是在途，落到下面按无留存记录处理。
        DeliveryTask active = ctx.deliveries.get(originalMessageId);
        if (active != null) {
            boolean inFlight;
            synchronized (active) {
                inFlight = active.state != DeliveryState.ACKED && active.state != DeliveryState.DEAD;
            }
            if (inFlight) {
                return rejectRedrive(null, subscriptionId, originalMessageId,
                        RedeliveryRejectReason.STILL_IN_FLIGHT);
            }
        }

        // 标记 REDRIVEN 与入队在 store 锁内原子完成：并发重投只有一个能成功；
        // 队列满时不改状态、不留副作用，记录保持 RETAINED 可稍后再试。
        synchronized (deadLetters) {
            DeadMessage rec = deadLetters.find(subscriptionId, originalMessageId);
            if (rec == null) {
                return rejectRedrive(null, subscriptionId, originalMessageId,
                        RedeliveryRejectReason.RECORD_NOT_FOUND);
            }
            if (rec.status() == DeadMessageStatus.REDRIVEN) {
                return rejectRedrive(rec.recordId(), subscriptionId, originalMessageId,
                        RedeliveryRejectReason.ALREADY_REDELIVERED);
            }
            Message fresh = Message.redelivered(rec.topic(), rec.payload(), rec.originMessageId());
            DeliveryTask task = new DeliveryTask(fresh);
            ctx.deliveries.put(fresh.id(), task);
            if (!ctx.queue.offer(task)) {
                ctx.deliveries.remove(fresh.id());
                stats.incEnqueueRejected();
                log.warn("[redrive-queue-full] sub={} origMsg={} record={} queueDepth={}",
                        subscriptionId, originalMessageId, rec.recordId(), ctx.queue.size());
                return rejectRedrive(rec.recordId(), subscriptionId, originalMessageId,
                        RedeliveryRejectReason.QUEUE_FULL);
            }
            // 持有 store 锁且 rec 为 RETAINED，标记必然成功（无并发归档/淘汰可能）
            DeadMessage marked = deadLetters.markRedelivered(rec, fresh.id());
            if (marked == null) {
                // 理论不可达：防御性撤回入队，按已重投挡下，保证不重复生效
                ctx.queue.remove(task);
                ctx.deliveries.remove(fresh.id());
                return rejectRedrive(rec.recordId(), subscriptionId, originalMessageId,
                        RedeliveryRejectReason.ALREADY_REDELIVERED);
            }
            stats.incEnqueued();
            stats.incReEnqueued();
            stats.incRedriveAccepted();
            log.info("[redrive] sub={} origMsg={} record={} newMsg={} originId={}",
                    subscriptionId, originalMessageId, rec.recordId(), fresh.id(), fresh.originId());
            return RedeliveryResult.accepted(rec.recordId(), fresh.id());
        }
    }

    private RedeliveryResult rejectRedrive(String recordId, String subscriptionId,
                                           String originalMessageId, RedeliveryRejectReason reason) {
        stats.incRedriveRejected();
        log.warn("[redrive-rejected] sub={} origMsg={} record={} reason={}",
                subscriptionId, originalMessageId, recordId, reason);
        return RedeliveryResult.rejected(recordId, reason);
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
                    // PENDING / BACKOFF 等非在途时刻到达的确认一律视为迟到，不改变状态
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
            long pending = 0, inFlight = 0, acked = 0, dead = 0, backoff = 0;
            for (DeliveryTask t : ctx.deliveries.values()) {
                switch (t.state) {
                    case PENDING -> pending++;
                    case IN_FLIGHT -> inFlight++;
                    case BACKOFF -> backoff++;
                    case ACKED -> acked++;
                    case DEAD -> dead++;
                }
            }
            m.put("statePending", pending);
            m.put("stateInFlight", inFlight);
            m.put("stateBackoff", backoff);
            m.put("stateAcked", acked);
            m.put("stateDead", dead);
            m.put("deadRetained", (long) deadLetters.retainedCountForSubscription(ctx.id));
            m.put("deadRedriven", (long) deadLetters.redrivenCountForSubscription(ctx.id));
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

        /**
         * 对单条消息执行 投递->等待确认->超时->退避->重试 直至 ACKED 或 DEAD。
         * 退避基于墙上时钟等待（Object.wait，非自旋、不空转 CPU），断连不打断退避，
         * 退避结束后由 awaitConnected 门控实际重投：断连则等待恢复，恢复后只投一次。
         */
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
                        task.state = DeliveryState.DEAD;
                        archiveDead(task, thisAttempt);
                        return;
                    }
                    // 进入退避等待，随后回到 PENDING 由下一轮重投
                    long backoff = config.backoffMillisForRetry(thisAttempt);
                    task.state = DeliveryState.BACKOFF;
                    stats.incRetries();
                    stats.incBackoffWaits();
                    stats.addBackoffMillis(backoff);
                    log.info("[retry-backoff] sub={} msg={} nextAttempt={} backoffMs={}",
                            id, task.message.id(), thisAttempt + 1, backoff);

                    long backoffDeadline = System.currentTimeMillis() + backoff;
                    while (task.state == DeliveryState.BACKOFF) {
                        long remain = backoffDeadline - System.currentTimeMillis();
                        if (remain <= 0) {
                            break;
                        }
                        task.wait(Math.min(remain, 50));
                    }
                    if (task.state == DeliveryState.DEAD) {
                        return; // 退避期间被 unsubscribe 确定终止
                    }
                    task.state = DeliveryState.PENDING;
                    log.info("[retry] sub={} msg={} nextAttempt={} reason=ack-timeout",
                            id, task.message.id(), thisAttempt + 1);
                }
            }
        }

        /** 重试耗尽：置 DEAD 并完整留存；调用时持有 task 监视器。 */
        private void archiveDead(DeliveryTask task, int attempts) {
            stats.incDead();
            int[] evicted = new int[1];
            DeadMessage rec = deadLetters.archive(
                    task.message.id(), task.message.originId(), id, task.message.topic(),
                    task.message.payload(), attempts, Instant.now(), evicted);
            for (int i = 0; i < evicted[0]; i++) {
                stats.incDeadEvicted();
            }
            if (rec != null) {
                stats.incDeadArchived();
            } else {
                stats.incDeadArchiveRejected();
            }
            deliveries.remove(task.message.id());
            log.warn("[dead] sub={} msg={} attempts={} reason=retries-exhausted retained={} record={}",
                    id, task.message.id(), attempts, rec != null, rec != null ? rec.recordId() : "-");
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
