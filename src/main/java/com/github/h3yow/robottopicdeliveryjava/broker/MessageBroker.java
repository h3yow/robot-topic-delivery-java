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
 * - 投递等待确认直至超时；超时后按可配置退避（逐次拉长）等待再重试，退避期间不占 CPU、
 *   不影响其他订阅；订阅断连/移除会即时唤醒等待，结果确定。
 * - ack 幂等：重复确认计 duplicateAcks，迟到确认（已 ACKED 之后按旧 attempt、或已 DEAD/已移除）
 *   计 lateAcks，均不改变终态。
 * - disconnect 暂停派发并保留全部未确认消息；reconnect 后补投，不静默丢失。
 * - 重试耗尽进入 DEAD 的消息进入失败留存（有界、可溢出取舍），可列出并按全新投递重投；
 *   旧记录永不复活，重投走完整 确认/超时/重试/终止 流程。
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
        this.deadLetters = new DeadLetterStore(config.deadLetterCapacity(),
                config.overflowPolicy(), stats);
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
        // 在途未确认的消息也计为 dropped（局部变量捕获，避免与派发线程置空之间的竞态）
        DeliveryTask inFlight = ctx.current;
        if (inFlight != null) {
            synchronized (inFlight) {
                if (inFlight.state == DeliveryState.IN_FLIGHT || inFlight.state == DeliveryState.PENDING) {
                    inFlight.state = DeliveryState.DEAD;
                    inFlight.notifyAll();
                    ctx.deliveries.remove(inFlight.message.id());
                    drained++;
                }
            }
            // 若该在途任务来自失败重投，把源留存推进到 REPLAYED（不复活、可审计）
            String origin = inFlight.message.originDeadLetterId();
            if (origin != null) {
                deadLetters.markReplayed(inFlight.message.id());
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

    // ---------------------------------------------------------------- 失败留存与重投

    /** 列出当前全部失败留存（按终结时间从老到新的不可变快照）。 */
    public List<DeadLetter> deadLetters() {
        return deadLetters.list();
    }

    /** 读取单条失败留存。 */
    public DeadLetter deadLetter(String deadLetterId) {
        return deadLetters.get(deadLetterId);
    }

    /**
     * 将一条失败留存按全新投递重新投递给其原本所属订阅。
     * 旧记录不复活：被接受的重投使源记录进入 REPLAYING/REPLAYED，重投消息是全新 id、
     * 重新走完整的 确认/超时/重试/终止 流程。
     */
    public ReplayResult replayDeadLetter(String deadLetterId) {
        if (closed) {
            log.warn("[replay] rejected reason={} letter={}", ReplayRejectReason.BROKER_CLOSED,
                    deadLetterId);
            return ReplayResult.rejected(deadLetterId, ReplayRejectReason.BROKER_CLOSED);
        }
        DeadLetter letter = deadLetters.get(deadLetterId);
        if (letter == null) {
            // 留存未落盘（DROP_NEWEST/容量淘汰）或正在与置 DEAD 同锁提交的极小窗口：
            // 若状态表里存在该消息的终态任务，建立 happens-before 后重读一次，消除竞态误判。
            if (!settlementInFlight(deadLetterId)) {
                return rejectReplay(deadLetterId, ReplayRejectReason.DEAD_LETTER_NOT_FOUND);
            }
            letter = deadLetters.get(deadLetterId);
            if (letter == null) {
                return rejectReplay(deadLetterId, ReplayRejectReason.DEAD_LETTER_NOT_FOUND);
            }
        }
        if (letter.state() != DeadLetterState.RETAINED) {
            ReplayRejectReason reason = letter.state() == DeadLetterState.REPLAYING
                    ? ReplayRejectReason.REPLAY_IN_FLIGHT
                    : ReplayRejectReason.ALREADY_REPLAYED;
            return rejectReplay(deadLetterId, reason);
        }
        SubscriptionContext ctx = subscriptions.get(letter.subscriptionId());
        if (ctx == null || ctx.removed) {
            return rejectReplay(deadLetterId, ReplayRejectReason.SUBSCRIPTION_NOT_FOUND);
        }

        // 全新消息：全新 id，主题/负载原样复制；先在存储层原子占位（RETAINED -> REPLAYING）
        Message replayed = Message.replayed(
                Topic.of(letter.topic()), letter.payload(), letter.id());
        DeadLetter claimed = deadLetters.markReplaying(deadLetterId, replayed.id());
        if (claimed == null) {
            // 并发重投：已被另一个请求占位（此刻只可能是在途状态）
            return rejectReplay(deadLetterId, ReplayRejectReason.REPLAY_IN_FLIGHT);
        }

        DeliveryTask task = new DeliveryTask(replayed);
        ctx.deliveries.put(replayed.id(), task);
        if (!ctx.queue.offer(task)) {
            // 队列满：沿用发布背压的确定性拒绝；回滚占位，不静默吞掉
            ctx.deliveries.remove(replayed.id());
            deadLetters.rollbackReplaying(replayed.id());
            stats.incEnqueueRejected();
            log.warn("[replay] rejected letter={} newMsg={} reason={} queueDepth={}",
                    deadLetterId, replayed.id(), ReplayRejectReason.QUEUE_FULL, ctx.queue.size());
            stats.incReplayRejected(ReplayRejectReason.QUEUE_FULL);
            return ReplayResult.rejected(deadLetterId, ReplayRejectReason.QUEUE_FULL);
        }
        stats.incEnqueued();
        stats.incReplayAccepted();
        log.info("[replay] accepted letter={} newMsg={} sub={} topic={}",
                deadLetterId, replayed.id(), ctx.id, letter.topic());
        return ReplayResult.accepted(deadLetterId, replayed.id());
    }

    /**
     * 以消息 id 在所有订阅的投递状态表中定位终态任务，并在其监视器上同步一次，
     * 确保与该任务 DEAD+留存的提交块建立 happens-before。返回是否找到终态任务。
     */
    private boolean settlementInFlight(String messageId) {
        for (SubscriptionContext ctx : subscriptions.values()) {
            DeliveryTask task = ctx.deliveries.get(messageId);
            if (task != null) {
                synchronized (task) {
                    return task.state == DeliveryState.DEAD
                            || task.state == DeliveryState.ACKED;
                }
            }
        }
        return false;
    }

    private ReplayResult rejectReplay(String deadLetterId, ReplayRejectReason reason) {
        stats.incReplayRejected(reason);
        log.warn("[replay] rejected letter={} reason={}", deadLetterId, reason);
        return ReplayResult.rejected(deadLetterId, reason);
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
        boolean validAck = false;
        synchronized (task) {
            switch (task.state) {
                case IN_FLIGHT -> {
                    if (attemptSeen == task.attempt) {
                        task.state = DeliveryState.ACKED; // 终态保留在状态表中，用于区分重复/迟到确认
                        stats.incAcked();
                        validAck = true;
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
        // 仅当重投消息被本次有效确认终结时，源留存才进入 REPLAYED（终态，不复活）
        if (validAck && task.message.originDeadLetterId() != null) {
            deadLetters.markReplayed(messageId);
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
        // 即时唤醒在 ack 超时等待或退避等待中的派发线程，不留下沉睡窗口
        DeliveryTask inFlight = ctx.current;
        if (inFlight != null) {
            synchronized (inFlight) {
                inFlight.notifyAll();
            }
        }
        synchronized (ctx.lifecycleLock) {
            ctx.lifecycleLock.notifyAll();
        }
        log.info("[disconnect] sub={} queueDepth={} inFlight={}",
                subscriptionId, ctx.queue.size(), inFlight != null ? 1 : 0);
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
        DeliveryTask inFlight = ctx.current;
        if (inFlight != null) {
            synchronized (inFlight) {
                inFlight.notifyAll();
            }
        }
        stats.addRedeliveredOnReconnect(unacked);
        log.info("[reconnect] sub={} promisedRedelivery={}", subscriptionId, unacked);
    }

    // ---------------------------------------------------------------- 统计与诊断

    public BrokerStats stats() {
        return stats;
    }

    /** 失败留存的全局诊断：当前留存数与容量上限、各处置状态数量。 */
    public Map<String, Long> deadLetterDiagnostics() {
        long retained = 0, replaying = 0, replayed = 0;
        for (DeadLetter letter : deadLetters.list()) {
            switch (letter.state()) {
                case RETAINED -> retained++;
                case REPLAYING -> replaying++;
                case REPLAYED -> replayed++;
            }
        }
        Map<String, Long> m = new ConcurrentHashMap<>();
        m.put("deadLetterCount", (long) deadLetters.size());
        m.put("deadLetterCapacity", (long) config.deadLetterCapacity());
        m.put("deadLetterStateRetained", retained);
        m.put("deadLetterStateReplaying", replaying);
        m.put("deadLetterStateReplayed", replayed);
        return m;
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
                // 排队中的重投消息被移除：源留存推进到 REPLAYED，不留新死信、不丢计数
                if (t.message.originDeadLetterId() != null) {
                    deadLetters.markReplayed(t.message.id());
                }
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
         * 退避等待：超时后按配置逐次拉长间隔，等待期间 wait 释放 CPU、不影响其他订阅。
         * 断连/移除/恢复会即时唤醒本等待；断连导致提前结束时调用方随后在 awaitConnected 停车，
         * 消息保持 PENDING、不重投、不丢失。
         */
        private void awaitBackoff(DeliveryTask task, int failedAttempt) throws InterruptedException {
            long delay = backoffDelayMillis(failedAttempt);
            if (delay <= 0) {
                return;
            }
            stats.incBackoffWaits();
            long deadline = System.currentTimeMillis() + delay;
            boolean interruptedEarly = false;
            String reason = null;
            synchronized (task) {
                while (task.state == DeliveryState.PENDING) {
                    if (removed || closed) {
                        interruptedEarly = true;
                        reason = "removed";
                        break;
                    }
                    long remain = deadline - System.currentTimeMillis();
                    if (remain <= 0) {
                        break;
                    }
                    if (!connected) {
                        interruptedEarly = true;
                        reason = "disconnected";
                        break;
                    }
                    task.wait(Math.min(remain, 50));
                }
            }
            if (interruptedEarly) {
                stats.incBackoffInterrupted();
                log.info("[backoff-interrupted] sub={} msg={} reason={}",
                        id, task.message.id(), reason);
            }
        }

        private long backoffDelayMillis(int failedAttempt) {
            long initial = config.retryBackoffInitialMillis();
            if (initial <= 0) {
                return 0L;
            }
            double raw = initial * Math.pow(config.retryBackoffMultiplier(), failedAttempt - 1);
            long delay = Double.isInfinite(raw) || raw > Long.MAX_VALUE
                    ? Long.MAX_VALUE : (long) raw;
            long max = config.retryBackoffMaxMillis();
            if (max > 0 && delay > max) {
                delay = max;
            }
            return Math.max(0L, delay);
        }

        /** 对单条消息执行 投递->等待确认->(退避)->重试 直至 ACKED 或 DEAD。 */
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
                        settleDead(task, thisAttempt);
                        log.warn("[dead] sub={} msg={} attempts={} reason=retries-exhausted retained={}",
                                id, task.message.id(), thisAttempt, deadLetters.get(task.message.id()) != null);
                        return;
                    }
                    task.state = DeliveryState.PENDING;
                    stats.incRetries();
                    log.info("[retry] sub={} msg={} nextAttempt={} reason=ack-timeout backoffMs={}",
                            id, task.message.id(), thisAttempt + 1,
                            backoffDelayMillis(thisAttempt));
                }
                // 退避在锁外等待（wait 本身释放 task 监视器），断连/移除可即时唤醒
                awaitBackoff(task, thisAttempt);
            }
        }

        /** 重试耗尽：在 task 监视器内留存记录（与置 DEAD 原子可见）。终态任务保留在状态表。 */
        private void settleDead(DeliveryTask task, int attempts) {
            DeadLetter letter = DeadLetter.retained(task.message, id, attempts,
                    Instant.now(), task.message.originDeadLetterId());
            deadLetters.retain(letter);
            // 若本次终止的是重投消息：源留存推进到 REPLAYED，本次留存是一条全新的可再重投记录
            if (task.message.originDeadLetterId() != null) {
                deadLetters.markReplayed(task.message.id());
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
