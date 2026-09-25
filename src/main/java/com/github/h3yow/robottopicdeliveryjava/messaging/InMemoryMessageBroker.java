package com.github.h3yow.robottopicdeliveryjava.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 纯内存、无外部依赖的可靠投递 Broker。
 *
 * <p>设计要点：
 * <ul>
 *   <li>每个订阅者一个有界队列 + 一个单线程 worker，保证该订阅者内严格按入队顺序投递；</li>
 *   <li>投递状态机收敛在 {@link DeliveryRecord}，所有迁移 CAS，重复/迟到确认幂等；</li>
 *   <li>全局单一超时扫描线程，把超时在途记录收回队首（退避后重投）；</li>
 *   <li>断开期间不调用处理器；恢复时所有在途记录无条件补投，不静默丢失；</li>
 *   <li>队列饱和按订阅者声明的 {@link BackpressurePolicy} 处理，结果计入统计。</li>
 * </ul>
 */
public class InMemoryMessageBroker implements MessageBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMessageBroker.class);

    private final BrokerProperties properties;
    private final MessageCodec codec;
    private final BrokerStats stats = new BrokerStats();

    private final Map<String, TopicDefinition> topics = new ConcurrentHashMap<>();
    private final Map<String, SubscriptionRuntime> subscriptions = new ConcurrentHashMap<>();

    private final AtomicLong messageIdSeq = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final java.util.UUID instanceId = java.util.UUID.randomUUID();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "broker-timeout-scanner");
                t.setDaemon(true);
                return t;
            });

    public InMemoryMessageBroker(BrokerProperties properties, MessageCodec codec) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.codec = Objects.requireNonNull(codec, "codec");
        long interval = Math.max(1L, properties.getTimeoutScanIntervalMillis());
        this.scheduler.scheduleWithFixedDelay(this::scanTimeouts, interval, interval, TimeUnit.MILLISECONDS);
        log.info("MessageBroker initialized: instance={} queueCapacity={}, policy={}, ackTimeoutMs={}, maxAttempts={}, graceMs={}",
                instanceId, properties.getQueueCapacity(), properties.getBackpressurePolicy(),
                properties.getAckTimeoutMillis(), properties.getMaxAttempts(),
                properties.getDisconnectGraceMillis());
    }

    // ---------------------------------------------------------------- topics

    @Override
    public Registration registerTopic(String topic, Class<?> payloadType, boolean requirePayload) {
        if (closed.get()) {
            return new Registration(RegistrationStatus.REJECTED, topic, "broker is closed");
        }
        String valid;
        try {
            valid = TopicMatcher.validateTopic(topic);
        } catch (RuntimeException e) {
            log.warn("registerTopic rejected: topic='{}' reason={}", topic, e.getMessage());
            return new Registration(RegistrationStatus.REJECTED, topic, e.getMessage());
        }
        if (payloadType == null) {
            return new Registration(RegistrationStatus.REJECTED, valid, "payloadType must not be null");
        }
        TopicDefinition definition = new TopicDefinition(valid, payloadType, requirePayload);
        // 同一主题重复注册必须显式拒绝，避免静默覆盖负载类型约束。
        if (topics.putIfAbsent(valid, definition) != null) {
            log.warn("registerTopic rejected: topic='{}' reason=already registered", valid);
            return new Registration(RegistrationStatus.REJECTED, valid, "topic already registered");
        }
        log.info("topic registered: topic='{}' payloadType={} requirePayload={}",
                valid, payloadType.getName(), requirePayload);
        return new Registration(RegistrationStatus.REGISTERED, valid, null);
    }

    // ------------------------------------------------------------ snapshot

    @Override
    public BrokerSnapshot snapshot() {
        List<SubscriptionSnapshot> list = new ArrayList<>();
        subscriptions.forEach((id, runtime) -> {
            synchronized (runtime) {
                list.add(new SubscriptionSnapshot(
                        id,
                        runtime.subscription.getFilter(),
                        runtime.connected,
                        runtime.queue.size(),
                        runtime.subscription.getQueueCapacity(),
                        runtime.localEnqueued.get(),
                        runtime.localAcked.get(),
                        runtime.localRetried.get(),
                        runtime.localDead.get(),
                        runtime.localDuplicateAcks.get(),
                        runtime.localRejected.get()));
            }
        });
        return new BrokerSnapshot(System.nanoTime(), stats, List.copyOf(list));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("MessageBroker closing: terminating {} subscription workers", subscriptions.size());
        scheduler.shutdownNow();
        List<SubscriptionRuntime> runtimes = List.copyOf(subscriptions.values());
        for (SubscriptionRuntime runtime : runtimes) {
            // 置移除标志 -> 终止未完成记录 -> 中断阻塞中的 worker。
            runtime.markRemoved();
            terminateRuntime(runtime, DeadReason.SUBSCRIBER_REMOVED);
            runtime.interruptWorker();
        }
        for (SubscriptionRuntime runtime : runtimes) {
            Thread w = runtime.worker;
            if (w != null) {
                try {
                    w.join(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        subscriptions.clear();
        log.info("MessageBroker closed");
    }

    /** 全局超时扫描：把超过 ackTimeout 的在途记录收回队首，安排退避后重试。 */
    private void scanTimeouts() {
        if (closed.get()) {
            return;
        }
        long deadlineNanos = System.nanoTime()
                - TimeUnit.MILLISECONDS.toNanos(properties.getAckTimeoutMillis());
        for (SubscriptionRuntime runtime : subscriptions.values()) {
            if (!runtime.connected) {
                continue;
            }
            for (DeliveryRecord record : List.copyOf(runtime.inflight)) {
                if (record.timeoutIfExpired(deadlineNanos)) {
                    int attempt = record.getAttempt();
                    log.warn("ack timeout, scheduling retry: subscriber='{}' messageId={} attempt={} timeoutMs={}",
                            runtime.subscription.getSubscriberId(), record.getMessageId(),
                            attempt, properties.getAckTimeoutMillis());
                    long backoff = backoffMillis(attempt);
                    record.setNotBeforeNanos(System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(backoff));
                    runtime.inflight.remove(record);
                    runtime.requeueHead(record);
                }
            }
        }
    }

    @Override
    public Registration subscribe(String subscriberId, String filter, MessageHandler handler) {
        return subscribe(subscriberId, filter, handler,
                properties.getQueueCapacity(), properties.getBackpressurePolicy());
    }

    @Override
    public Registration subscribe(String subscriberId, String filter, MessageHandler handler,
                                  int queueCapacity, BackpressurePolicy policy) {
        if (closed.get()) {
            return new Registration(RegistrationStatus.REJECTED, subscriberId, "broker is closed");
        }
        if (subscriberId == null || subscriberId.isEmpty()) {
            return new Registration(RegistrationStatus.REJECTED, String.valueOf(subscriberId),
                    "subscriberId must not be null or empty");
        }
        if (handler == null) {
            return new Registration(RegistrationStatus.REJECTED, subscriberId, "handler must not be null");
        }
        String validFilter;
        try {
            validFilter = TopicMatcher.validateFilter(filter);
        } catch (RuntimeException e) {
            log.warn("subscribe rejected: subscriber='{}' filter='{}' reason={}",
                    subscriberId, filter, e.getMessage());
            return new Registration(RegistrationStatus.REJECTED, String.valueOf(filter), e.getMessage());
        }
        BackpressurePolicy effectivePolicy = policy != null ? policy : properties.getBackpressurePolicy();
        int effectiveCapacity = queueCapacity > 0 ? queueCapacity : properties.getQueueCapacity();
        Subscription subscription = new Subscription(
                subscriberId, validFilter, handler, effectiveCapacity, effectivePolicy);
        SubscriptionRuntime runtime = new SubscriptionRuntime(subscription);
        if (subscriptions.putIfAbsent(subscriberId, runtime) != null) {
            log.warn("subscribe rejected: subscriber='{}' reason=already subscribed", subscriberId);
            return new Registration(RegistrationStatus.REJECTED, subscriberId,
                    "subscriberId already subscribed");
        }
        runtime.start();
        log.info("subscriber added: subscriber='{}' filter='{}' capacity={} policy={}",
                subscriberId, validFilter, effectiveCapacity, effectivePolicy);
        return new Registration(RegistrationStatus.REGISTERED, subscriberId, null);
    }

    @Override
    public void unsubscribe(String subscriberId) {
        SubscriptionRuntime runtime = subscriptions.remove(subscriberId);
        if (runtime == null) {
            log.warn("unsubscribe ignored: subscriber='{}' reason=not found", subscriberId);
            return;
        }
        // 移除是确定的：顺序固定为 置移除标志 -> 终止全部未完成记录 -> 中断 worker。
        // 这样处理器在中断后“补确认”时记录已是 DEAD，ack 必被拒绝（计迟到确认），
        // 已 ACKED 的记录 markDead 返回 false，不受影响（计数守恒）。
        runtime.markRemoved();
        terminateRuntime(runtime, DeadReason.SUBSCRIBER_REMOVED);
        runtime.interruptWorker();
        log.info("subscriber removed: subscriber='{}' terminatedPending={}",
                subscriberId, runtime.localDead.get());
    }

    @Override
    public void disconnect(String subscriberId) {
        SubscriptionRuntime runtime = subscriptions.get(subscriberId);
        if (runtime == null) {
            log.warn("disconnect ignored: subscriber='{}' reason=not found", subscriberId);
            return;
        }
        synchronized (runtime) {
            if (runtime.connected) {
                runtime.connected = false;
                runtime.disconnectedAtNanos = System.nanoTime();
                // 在途消息立即收回为 PENDING：恢复时一定补投；超过宽限期则确定终止。
                int reclaimed = 0;
                for (DeliveryRecord record : List.copyOf(runtime.inflight)) {
                    if (record.reclaimInflight()) {
                        reclaimed++;
                        runtime.queue.addFirst(record);
                    }
                }
                runtime.inflight.clear();
                log.info("subscriber disconnected: subscriber='{}' reclaimedInflight={} queued={} graceMs={}",
                        subscriberId, reclaimed, runtime.queue.size(),
                        properties.getDisconnectGraceMillis());
            }
        }
    }

    @Override
    public void recover(String subscriberId) {
        SubscriptionRuntime runtime = subscriptions.get(subscriberId);
        if (runtime == null) {
            log.warn("recover ignored: subscriber='{}' reason=not found", subscriberId);
            return;
        }
        synchronized (runtime) {
            if (!runtime.connected) {
                runtime.connected = true;
                // 不静默丢失：断开期间被收回的消息已在队首；清空退避，worker 立即补投。
                for (DeliveryRecord record : runtime.queue) {
                    record.setNotBeforeNanos(0L);
                }
                log.info("subscriber recovered: subscriber='{}' pendingRedelivery={}",
                        subscriberId, runtime.queue.size());
                runtime.notifyAll();
            }
        }
    }

    @Override
    public PublishReceipt publish(String topic, Object payload) {
        if (closed.get()) {
            stats.incrementPublishedRejected();
            return new PublishReceipt(PublishStatus.REJECTED, null, "broker is closed");
        }
        String valid;
        try {
            valid = TopicMatcher.validateTopic(topic);
        } catch (RuntimeException e) {
            stats.incrementPublishedRejected();
            log.warn("publish rejected: topic='{}' reason=INVALID_TOPIC detail={}", topic, e.getMessage());
            return new PublishReceipt(PublishStatus.REJECTED, null, "INVALID_TOPIC: " + e.getMessage());
        }
        TopicDefinition definition = topics.get(valid);
        if (definition == null) {
            stats.incrementPublishedRejected();
            log.warn("publish rejected: topic='{}' reason=UNKNOWN_TOPIC", valid);
            return new PublishReceipt(PublishStatus.REJECTED, null,
                    "UNKNOWN_TOPIC: topic '" + valid + "' is not registered");
        }
        if (payload == null) {
            if (definition.requirePayload()) {
                stats.incrementPublishedRejected();
                log.warn("publish rejected: topic='{}' reason=INVALID_PAYLOAD detail=null payload not allowed", valid);
                return new PublishReceipt(PublishStatus.REJECTED, null,
                        "INVALID_PAYLOAD: topic '" + valid + "' requires a non-null payload");
            }
        } else if (!definition.payloadType().isInstance(payload)) {
            stats.incrementPublishedRejected();
            log.warn("publish rejected: topic='{}' reason=INVALID_PAYLOAD detail=expected {} got {}",
                    valid, definition.payloadType().getName(), payload.getClass().getName());
            return new PublishReceipt(PublishStatus.REJECTED, null,
                    "INVALID_PAYLOAD: topic '" + valid + "' expects "
                            + definition.payloadType().getName() + " but got "
                            + payload.getClass().getName());
        }

        final byte[] encoded;
        try {
            encoded = codec.encode(payload);
        } catch (RuntimeException e) {
            stats.incrementPublishedRejected();
            log.warn("publish rejected: topic='{}' reason=ENCODING_FAILURE detail={}",
                    valid, e.getMessage());
            return new PublishReceipt(PublishStatus.REJECTED, null,
                    "ENCODING_FAILURE: " + e.getMessage());
        }

        String messageId = "msg-" + messageIdSeq.incrementAndGet();
        PublishedMessage message = new PublishedMessage(messageId, valid, payload, encoded, System.nanoTime());

        // 快照式匹配：发布瞬间命中的订阅者集合是确定的；之后新增/移除互不影响本次投递。
        List<SubscriptionRuntime> matched = new ArrayList<>();
        for (SubscriptionRuntime runtime : subscriptions.values()) {
            if (TopicMatcher.matches(runtime.subscription.getFilter(), valid)) {
                matched.add(runtime);
            }
        }
        if (matched.isEmpty()) {
            stats.incrementPublishedNoSubscriber();
            // 没有订阅者不算“失败”：消息被接受（已校验并编码），只是无投递对象，原因可解释。
            stats.incrementPublishedAccepted();
            log.info("publish accepted with no subscriber: topic='{}' messageId={}", valid, messageId);
            return new PublishReceipt(PublishStatus.ACCEPTED, messageId, "NO_SUBSCRIBER");
        }

        List<String> rejections = new ArrayList<>();
        for (SubscriptionRuntime runtime : matched) {
            DeliveryRecord record = new DeliveryRecord(
                    runtime.subscription.getSubscriberId(), message, properties.getMaxAttempts());
            EnqueueResult result = runtime.enqueueTail(record);
            if (!result.isOk()) {
                rejections.add(runtime.subscription.getSubscriberId() + ": " + result.getReason());
            }
        }
        if (!rejections.isEmpty()) {
            stats.incrementPublishedRejected();
            String reason = "ENQUEUE_REJECTED: " + String.join("; ", rejections);
            log.warn("publish partially rejected: topic='{}' messageId={} matched={} rejected={}",
                    valid, messageId, matched.size(), rejections.size());
            return new PublishReceipt(PublishStatus.REJECTED, messageId, reason);
        }
        stats.incrementPublishedAccepted();
        log.info("publish accepted: topic='{}' messageId={} subscribers={}",
                valid, messageId, matched.size());
        return new PublishReceipt(PublishStatus.ACCEPTED, messageId, null);
    }

    @Override
    public BatchPublishResult publishBatch(List<BatchItem> items) {
        Objects.requireNonNull(items, "items");
        List<PublishReceipt> receipts = new ArrayList<>(items.size());
        for (BatchItem item : items) {
            // 逐条调用 publish：任何一条被拒绝都不影响同批其他消息或其他订阅者。
            receipts.add(publish(item.topic(), item.payload()));
        }
        BatchPublishResult result = new BatchPublishResult(List.copyOf(receipts));
        log.info("batch publish finished: total={} accepted={} rejected={}",
                result.receipts().size(), result.acceptedCount(), result.rejectedCount());
        return result;
    }

    /** 终止某订阅运行时中所有未完成（未 ACKED）的记录。 */
    private void terminateRuntime(SubscriptionRuntime runtime, DeadReason reason) {
        for (DeliveryRecord record : List.copyOf(runtime.inflight)) {
            if (record.markDead(reason)) {
                runtime.localDead.incrementAndGet();
                stats.incrementDead(reason);
            }
        }
        runtime.inflight.clear();
        List<DeliveryRecord> pending = new ArrayList<>();
        runtime.queue.drainTo(pending);
        for (DeliveryRecord record : pending) {
            if (record.markDead(reason)) {
                runtime.localDead.incrementAndGet();
                stats.incrementDead(reason);
                log.warn("delivery dead: subscriber='{}' messageId={} reason={}",
                        runtime.subscription.getSubscriberId(), record.getMessageId(), reason);
            }
        }
    }

    /**
     * 单个订阅者的运行时：有界双端队列 + 单线程 worker。
     * 队列用 {@link LinkedBlockingDeque}，超时/恢复的重试走 addFirst（保持在队首）。
     */
    private final class SubscriptionRuntime implements Runnable {

        private final Subscription subscription;
        private final LinkedBlockingDeque<DeliveryRecord> queue;
        /** 当前在途（已发出、等待确认）的记录，供超时扫描与恢复使用。 */
        private final java.util.Set<DeliveryRecord> inflight =
                java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
        private volatile boolean connected = true;
        private volatile long disconnectedAtNanos;
        private volatile boolean removed = false;

        private final AtomicLong localEnqueued = new AtomicLong();
        private final AtomicLong localAcked = new AtomicLong();
        private final AtomicLong localRetried = new AtomicLong();
        private final AtomicLong localDead = new AtomicLong();
        private final AtomicLong localDuplicateAcks = new AtomicLong();
        private final AtomicLong localRejected = new AtomicLong();

        private Thread worker;

        private SubscriptionRuntime(Subscription subscription) {
            this.subscription = subscription;
            this.queue = new LinkedBlockingDeque<>(subscription.getQueueCapacity());
        }

        private void start() {
            worker = new Thread(this, "subscriber-worker-" + subscription.getSubscriberId());
            worker.setDaemon(true);
            worker.start();
        }

        void markRemoved() {
            // 先停止 worker 循环的后续派发，再由调用方终止记录，最后中断阻塞中的线程。
            removed = true;
        }

        void interruptWorker() {
            Thread w = worker;
            if (w != null) {
                w.interrupt();
            }
        }

        /**
         * 新消息入队尾，按订阅者的背压策略处理饱和。
         * 仅在 Broker 全局锁保护订阅列表的窗口之外由发布线程调用。
         */
        private EnqueueResult enqueueTail(DeliveryRecord record) {
            int capacity = subscription.getQueueCapacity();
            switch (subscription.getBackpressurePolicy()) {
                case BLOCK -> {
                    try {
                        queue.put(record);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        record.markDead(DeadReason.SUBSCRIBER_REMOVED);
                        return EnqueueResult.rejected("interrupted while blocked on full queue");
                    }
                    afterEnqueued(record);
                    return EnqueueResult.ok();
                }
                case REJECT -> {
                    if (!queue.offer(record)) {
                        localRejected.incrementAndGet();
                        stats.incrementRejectedEnqueues();
                        record.markDead(DeadReason.QUEUE_FULL_REJECTED);
                        localDead.incrementAndGet();
                        stats.incrementDead(DeadReason.QUEUE_FULL_REJECTED);
                        log.warn("enqueue rejected: subscriber='{}' messageId={} queue={}/{} policy=REJECT",
                                subscription.getSubscriberId(), record.getMessageId(), queue.size(), capacity);
                        return EnqueueResult.rejected(
                                "subscriber queue full (" + queue.size() + "/" + capacity + "), REJECT policy");
                    }
                    afterEnqueued(record);
                    return EnqueueResult.ok();
                }
                case DROP_OLDEST -> {
                    DeliveryRecord victim = null;
                    synchronized (this) {
                        if (!queue.offer(record)) {
                            victim = queue.poll();
                            if (victim != null) {
                                queue.addLast(record);
                            }
                        }
                    }
                    if (victim != null) {
                        if (victim.markDead(DeadReason.DROPPED_OLDEST)) {
                            localDead.incrementAndGet();
                            stats.incrementDead(DeadReason.DROPPED_OLDEST);
                            log.warn("backpressure drop-oldest: subscriber='{}' dropped messageId={} for new messageId={} queue={}/{}",
                                    subscription.getSubscriberId(), victim.getMessageId(),
                                    record.getMessageId(), queue.size(), capacity);
                        }
                    }
                    afterEnqueued(record);
                    return EnqueueResult.ok();
                }
                default -> throw new IllegalStateException(
                        "unknown backpressure policy: " + subscription.getBackpressurePolicy());
            }
        }

        /** 超时/恢复/否定确认后的重试：放回队首，排在尚未发出的消息之前。 */
        private void requeueHead(DeliveryRecord record) {
            // 容量满时重试必须保证承诺不丢：自旋等待空位（重试消息只有 1 条在途，不会永久饥饿）。
            while (!removed && !closed.get()) {
                synchronized (this) {
                    if (queue.offerFirst(record)) {
                        localRetried.incrementAndGet();
                        stats.incrementRetries();
                        return;
                    }
                }
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        private void afterEnqueued(DeliveryRecord record) {
            localEnqueued.incrementAndGet();
            stats.incrementDeliveriesEnqueued();
        }

        @Override
        public void run() {
            log.info("worker started: subscriber='{}' filter='{}' capacity={} policy={}",
                    subscription.getSubscriberId(), subscription.getFilter(),
                    subscription.getQueueCapacity(), subscription.getBackpressurePolicy());
            while (!removed && !closed.get()) {
                try {
                    DeliveryRecord record = queue.take();
                    dispatchOne(record);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable t) {
                    // worker 绝不允许因单条处理异常而静默死亡。
                    log.error("worker unexpected error: subscriber='{}' error={}",
                            subscription.getSubscriberId(), t.toString(), t);
                }
            }
            log.info("worker stopped: subscriber='{}' remainingQueue={}",
                    subscription.getSubscriberId(), queue.size());
        }

        private void dispatchOne(DeliveryRecord record) throws InterruptedException {
            // 退避等待（超时重试），且断开期间暂停消费。
            long waitNanos = record.getNotBeforeNanos() - System.nanoTime();
            if (waitNanos > 0) {
                Thread.sleep(Math.max(1L, waitNanos / 1_000_000L));
            }
            if (!connected) {
                handleWhileDisconnected(record);
                return;
            }
            long now = System.nanoTime();
            int alreadyAttempted = record.getAttempt();
            if (alreadyAttempted >= properties.getMaxAttempts()) {
                if (record.markDead(DeadReason.RETRIES_EXHAUSTED)) {
                    localDead.incrementAndGet();
                    stats.incrementDead(DeadReason.RETRIES_EXHAUSTED);
                    log.warn("delivery dead: subscriber='{}' messageId={} reason=RETRIES_EXHAUSTED maxAttempts={}",
                            subscription.getSubscriberId(), record.getMessageId(),
                            properties.getMaxAttempts());
                }
                return;
            }
            int attempt = record.beginDispatch(now);
            if (attempt < 0) {
                // 竞态保护：记录已终止/确认（如退避等待期间被移除），或已达最大尝试次数（上面已处理）。
                if (record.getState() != DeliveryState.DEAD) {
                    log.debug("skip dispatch: subscriber='{}' messageId={} state={}",
                            subscription.getSubscriberId(), record.getMessageId(), record.getState());
                }
                return;
            }

            DeliveryControlImpl control = new DeliveryControlImpl(record, attempt);
            DeliveredMessageView view = new DeliveredMessageView(record, attempt);
            // 必须先进入在途集合再可能被 disconnect 观察到，避免“既不在队列也不在在途”的窗口。
            inflight.add(record);
            // 二次确认：beginDispatch 之后、add 之前若 disconnect 已把记录收回为 PENDING，
            // 则放弃本次派发（记录已由 disconnect 放回队首；此处只做兜底，不计重试）。
            if (!connected || record.getState() != DeliveryState.IN_FLIGHT) {
                inflight.remove(record);
                if (record.getState() == DeliveryState.PENDING) {
                    queue.addFirst(record);
                }
                return;
            }
            log.info("dispatch: subscriber='{}' messageId={} topic='{}' attempt={}",
                    subscription.getSubscriberId(), record.getMessageId(),
                    record.getMessage().getTopic(), attempt);
            try {
                subscription.getHandler().handle(view, control);
            } catch (Throwable t) {
                log.warn("handler threw: subscriber='{}' messageId={} attempt={} error={}",
                        subscription.getSubscriberId(), record.getMessageId(), attempt, t.toString());
                control.nack();
                inflight.remove(record);
                return;
            }
            // ack 后记录离开在途集合；nack 已重入队首；
            // 未决（既未 ack 也未 nack）必须保留在 inflight 中等待超时扫描。
            if (control.decided()) {
                inflight.remove(record);
            } else {
                log.debug("handler returned without decision, awaiting ack timeout: subscriber='{}' messageId={} attempt={}",
                        subscription.getSubscriberId(), record.getMessageId(), attempt);
            }
        }

        private void handleWhileDisconnected(DeliveryRecord record) throws InterruptedException {
            long graceNanos = TimeUnit.MILLISECONDS.toNanos(properties.getDisconnectGraceMillis());
            long deadline = disconnectedAtNanos + graceNanos;
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) {
                // 放回队首等待恢复或宽限期到，避免忙等。
                queue.putFirst(record);
                Thread.sleep(Math.min(20L, Math.max(1L, remaining / 1_000_000L)));
                return;
            }
            if (record.markDead(DeadReason.CONNECTION_LOST)) {
                localDead.incrementAndGet();
                stats.incrementDead(DeadReason.CONNECTION_LOST);
                log.warn("delivery dead: subscriber='{}' messageId={} reason=CONNECTION_LOST graceMs={} exceeded",
                        subscription.getSubscriberId(), record.getMessageId(),
                        properties.getDisconnectGraceMillis());
            }
        }

        private final class DeliveryControlImpl implements MessageHandler.DeliveryControl {

            private final DeliveryRecord record;
            private final int tokenAttempt;
            private volatile boolean decided = false;

            private DeliveryControlImpl(DeliveryRecord record, int tokenAttempt) {
                this.record = record;
                this.tokenAttempt = tokenAttempt;
            }

            boolean decided() {
                return decided;
            }

            @Override
            public boolean ack() {
                DeliveryRecord.AckOutcome outcome = record.ack(tokenAttempt);
                switch (outcome) {
                    case ACKED -> {
                        decided = true;
                        localAcked.incrementAndGet();
                        stats.incrementDeliveriesAcked();
                        log.info("ack accepted: subscriber='{}' messageId={} attempt={}",
                                subscription.getSubscriberId(), record.getMessageId(), tokenAttempt);
                        return true;
                    }
                    case DUPLICATE -> {
                        localDuplicateAcks.incrementAndGet();
                        stats.incrementDuplicateAcks();
                        log.warn("duplicate ack ignored: subscriber='{}' messageId={} attempt={} state=ACKED",
                                subscription.getSubscriberId(), record.getMessageId(), tokenAttempt);
                        return false;
                    }
                    case LATE -> {
                        localDuplicateAcks.incrementAndGet();
                        stats.incrementLateAcks();
                        log.warn("late/out-of-order ack ignored: subscriber='{}' messageId={} attempt={} state={}",
                                subscription.getSubscriberId(), record.getMessageId(),
                                tokenAttempt, record.getState());
                        return false;
                    }
                    default -> throw new IllegalStateException("unknown ack outcome: " + outcome);
                }
            }

            @Override
            public boolean nack() {
                if (!decided && record.nack(tokenAttempt)) {
                    decided = true;
                    long backoffMillis = backoffMillis(tokenAttempt);
                    record.setNotBeforeNanos(System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(backoffMillis));
                    requeueHead(record);
                    log.info("nack -> retry scheduled: subscriber='{}' messageId={} attempt={} backoffMs={}",
                            subscription.getSubscriberId(), record.getMessageId(), tokenAttempt, backoffMillis);
                    return true;
                }
                return false;
            }
        }
    }

    /** 投递给处理器的消息视图：payload 从已编码字节解码，保证恢复后语义一致。 */
    private record DeliveredMessageView(String messageId, String topic, Object payload, int attempt)
            implements MessageHandler.DeliveredMessage {

        DeliveredMessageView(DeliveryRecord record, int attempt) {
            this(record.getMessageId(), record.getMessage().getTopic(), record.getMessage().getPayload(), attempt);
        }
    }

    /** 入队结果。 */
    private static final class EnqueueResult {
        private final boolean ok;
        private final String reason;

        private EnqueueResult(boolean ok, String reason) {
            this.ok = ok;
            this.reason = reason;
        }

        static EnqueueResult ok() {
            return new EnqueueResult(true, null);
        }

        static EnqueueResult rejected(String reason) {
            return new EnqueueResult(false, reason);
        }

        boolean isOk() {
            return ok;
        }

        String getReason() {
            return reason;
        }
    }

    private long backoffMillis(int attempt) {
        long base = Math.max(1L, properties.getAckTimeoutMillis() / 5);
        return Math.min(30_000L, base * (1L << Math.min(attempt - 1, 10)));
    }
}
