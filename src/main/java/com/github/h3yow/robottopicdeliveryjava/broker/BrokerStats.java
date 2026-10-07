package com.github.h3yow.robottopicdeliveryjava.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可核验的运行统计。所有计数单调递增，守恒关系：
 *   enqueued == acked + dead + dropped + (pending + inFlight)
 * 其中 pending+inFlight 为仍在途数量，可由 snapshot() 中的在途计数验证。
 *
 * 失败留存与重投的记账：
 * - 每条进入 DEAD 终态的投递先计入 dead；被留存的再计入 deadLetterRetained，
 *   容量溢出被淘汰/拒绝留存的计入 deadLetterEvictedOldest / deadLetterEvictedNewest。
 * - 重投被接受计入 replayAccepted 并同时计入 enqueued（重投是一次全新投递）；
 *   被挡下按原因计入 replayRejected*；因队列满挡下时同时计入 enqueueRejected。
 */
public class BrokerStats {

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishRejected = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong enqueueRejected = new AtomicLong(); // 背压拒绝（含重投撞队列满）
    private final AtomicLong delivered = new AtomicLong();       // 实际调用监听器的次数（含重投）
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong duplicateAcks = new AtomicLong();
    private final AtomicLong lateAcks = new AtomicLong();        // 已终止后收到的确认
    private final AtomicLong timeouts = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong dead = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();         // 订阅被移除时丢弃的在途消息
    private final AtomicLong redeliveredOnReconnect = new AtomicLong();

    private final AtomicLong deadLetterRetained = new AtomicLong();
    private final AtomicLong deadLetterEvictedOldest = new AtomicLong(); // DROP_OLDEST 淘汰的老记录数
    private final AtomicLong deadLetterEvictedNewest = new AtomicLong(); // DROP_NEWEST 下新记录被拒留数
    private final AtomicLong replayAccepted = new AtomicLong();
    private final AtomicLong replayRejectedNotFound = new AtomicLong();
    private final AtomicLong replayRejectedInFlight = new AtomicLong();
    private final AtomicLong replayRejectedAlreadyReplayed = new AtomicLong();
    private final AtomicLong replayRejectedSubscriptionNotFound = new AtomicLong();
    private final AtomicLong replayRejectedQueueFull = new AtomicLong();
    private final AtomicLong backoffWaits = new AtomicLong();        // 进入正退避等待的次数
    private final AtomicLong backoffInterrupted = new AtomicLong();  // 退避等待被断连/移除提前唤醒的次数

    public void incPublished() { published.incrementAndGet(); }
    public void incPublishRejected() { publishRejected.incrementAndGet(); }
    public void incEnqueued() { enqueued.incrementAndGet(); }
    public void incEnqueueRejected() { enqueueRejected.incrementAndGet(); }
    public void incDelivered() { delivered.incrementAndGet(); }
    public void incAcked() { acked.incrementAndGet(); }
    public void incDuplicateAcks() { duplicateAcks.incrementAndGet(); }
    public void incLateAcks() { lateAcks.incrementAndGet(); }
    public void incTimeouts() { timeouts.incrementAndGet(); }
    public void incRetries() { retries.incrementAndGet(); }
    public void incDead() { dead.incrementAndGet(); }
    public void incDropped() { dropped.incrementAndGet(); }
    public void incRedeliveredOnReconnect() { redeliveredOnReconnect.incrementAndGet(); }
    public void addRedeliveredOnReconnect(long n) { redeliveredOnReconnect.addAndGet(n); }

    public void incDeadLetterRetained() { deadLetterRetained.incrementAndGet(); }
    public void incDeadLetterEvictedOldest() { deadLetterEvictedOldest.incrementAndGet(); }
    public void incDeadLetterEvictedNewest() { deadLetterEvictedNewest.incrementAndGet(); }
    public void incReplayAccepted() { replayAccepted.incrementAndGet(); }
    public void incReplayRejected(ReplayRejectReason reason) {
        switch (reason) {
            case DEAD_LETTER_NOT_FOUND -> replayRejectedNotFound.incrementAndGet();
            case REPLAY_IN_FLIGHT -> replayRejectedInFlight.incrementAndGet();
            case ALREADY_REPLAYED -> replayRejectedAlreadyReplayed.incrementAndGet();
            case SUBSCRIPTION_NOT_FOUND -> replayRejectedSubscriptionNotFound.incrementAndGet();
            case QUEUE_FULL -> replayRejectedQueueFull.incrementAndGet();
            case BROKER_CLOSED -> { /* broker 关闭不单独计数 */ }
        }
    }
    public void incBackoffWaits() { backoffWaits.incrementAndGet(); }
    public void incBackoffInterrupted() { backoffInterrupted.incrementAndGet(); }

    public Map<String, Long> snapshot() {
        Map<String, Long> m = new ConcurrentHashMap<>();
        m.put("published", published.get());
        m.put("publishRejected", publishRejected.get());
        m.put("enqueued", enqueued.get());
        m.put("enqueueRejected", enqueueRejected.get());
        m.put("delivered", delivered.get());
        m.put("acked", acked.get());
        m.put("duplicateAcks", duplicateAcks.get());
        m.put("lateAcks", lateAcks.get());
        m.put("timeouts", timeouts.get());
        m.put("retries", retries.get());
        m.put("dead", dead.get());
        m.put("dropped", dropped.get());
        m.put("redeliveredOnReconnect", redeliveredOnReconnect.get());
        m.put("deadLetterRetained", deadLetterRetained.get());
        m.put("deadLetterEvictedOldest", deadLetterEvictedOldest.get());
        m.put("deadLetterEvictedNewest", deadLetterEvictedNewest.get());
        m.put("replayAccepted", replayAccepted.get());
        m.put("replayRejectedNotFound", replayRejectedNotFound.get());
        m.put("replayRejectedInFlight", replayRejectedInFlight.get());
        m.put("replayRejectedAlreadyReplayed", replayRejectedAlreadyReplayed.get());
        m.put("replayRejectedSubscriptionNotFound", replayRejectedSubscriptionNotFound.get());
        m.put("replayRejectedQueueFull", replayRejectedQueueFull.get());
        m.put("backoffWaits", backoffWaits.get());
        m.put("backoffInterrupted", backoffInterrupted.get());
        return m;
    }
}
