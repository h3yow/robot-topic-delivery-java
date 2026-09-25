package com.github.h3yow.robottopicdeliveryjava.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可核验的运行统计。所有计数单调递增，守恒关系：
 *   enqueued == acked + dead + dropped + (pending + inFlight)
 * 其中 pending+inFlight 为仍在途数量，可由 snapshot() 中的在途计数验证。
 */
public class BrokerStats {

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishRejected = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong enqueueRejected = new AtomicLong(); // 背压拒绝
    private final AtomicLong delivered = new AtomicLong();       // 实际调用监听器的次数（含重投）
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong duplicateAcks = new AtomicLong();
    private final AtomicLong lateAcks = new AtomicLong();        // 已终止后收到的确认
    private final AtomicLong timeouts = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong dead = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();         // 订阅被移除时丢弃的在途消息
    private final AtomicLong redeliveredOnReconnect = new AtomicLong();

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
        return m;
    }
}
