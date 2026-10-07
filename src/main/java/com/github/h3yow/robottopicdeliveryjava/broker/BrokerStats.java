package com.github.h3yow.robottopicdeliveryjava.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可核验的运行统计。所有计数单调递增。
 *
 * 每批入队（含重投的 reEnqueued）的逐消息守恒：
 *   enqueued == acked + dead + dropped + (pending + inFlight + backoff)
 * 其中右侧在途项可由 snapshot() 计数与 diagnostics() 的状态计数验证。
 *
 * 死信侧记账：
 *   dead == deadArchived + deadArchiveRejected
 *   deadArchived == deadRetained + deadRedriven + deadEvicted  （任意时刻，Redrive 后会被 EVICT 的情况除外——
 *   见 DeadLetterStore 说明；本实现 REDRIVEN 记录仍占容量并继续留存，故该式恒成立）
 */
public class BrokerStats {

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishRejected = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong enqueueRejected = new AtomicLong(); // 背压拒绝（含重投撞队列满）
    private final AtomicLong delivered = new AtomicLong();       // 实际调用监听器的次数（含重试/重投）
    private final AtomicLong acked = new AtomicLong();
    private final AtomicLong duplicateAcks = new AtomicLong();
    private final AtomicLong lateAcks = new AtomicLong();        // 已终止后收到的确认
    private final AtomicLong timeouts = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong backoffWaits = new AtomicLong();    // 进入退避等待的次数
    private final AtomicLong backoffMillis = new AtomicLong();   // 累计计划退避等待毫秒
    private final AtomicLong dead = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();         // 订阅被移除时丢弃的在途消息
    private final AtomicLong redeliveredOnReconnect = new AtomicLong();

    // ---- 死信留存与重投 ----
    private final AtomicLong deadArchived = new AtomicLong();        // 成功留存的终止消息
    private final AtomicLong deadArchiveRejected = new AtomicLong(); // 容量满(REJECT_NEW)未留存
    private final AtomicLong deadEvicted = new AtomicLong();         // EVICT_OLDEST 淘汰的记录
    private final AtomicLong redriveRequested = new AtomicLong();    // 重投请求总数
    private final AtomicLong redriveAccepted = new AtomicLong();     // 重投被接受并重新入队
    private final AtomicLong redriveRejected = new AtomicLong();     // 重投被挡下（原因可区分）
    private final AtomicLong reEnqueued = new AtomicLong();          // 重投产生的全新投递的入队数

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
    public void incBackoffWaits() { backoffWaits.incrementAndGet(); }
    public void addBackoffMillis(long n) { if (n > 0) backoffMillis.addAndGet(n); }
    public void incDead() { dead.incrementAndGet(); }
    public void incDropped() { dropped.incrementAndGet(); }
    public void incRedeliveredOnReconnect() { redeliveredOnReconnect.incrementAndGet(); }
    public void addRedeliveredOnReconnect(long n) { redeliveredOnReconnect.addAndGet(n); }

    public void incDeadArchived() { deadArchived.incrementAndGet(); }
    public void incDeadArchiveRejected() { deadArchiveRejected.incrementAndGet(); }
    public void incDeadEvicted() { deadEvicted.incrementAndGet(); }
    public void incRedriveRequested() { redriveRequested.incrementAndGet(); }
    public void incRedriveAccepted() { redriveAccepted.incrementAndGet(); }
    public void incRedriveRejected() { redriveRejected.incrementAndGet(); }
    public void incReEnqueued() { reEnqueued.incrementAndGet(); }

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
        m.put("backoffWaits", backoffWaits.get());
        m.put("backoffMillis", backoffMillis.get());
        m.put("dead", dead.get());
        m.put("dropped", dropped.get());
        m.put("redeliveredOnReconnect", redeliveredOnReconnect.get());
        m.put("deadArchived", deadArchived.get());
        m.put("deadArchiveRejected", deadArchiveRejected.get());
        m.put("deadEvicted", deadEvicted.get());
        m.put("redriveRequested", redriveRequested.get());
        m.put("redriveAccepted", redriveAccepted.get());
        m.put("redriveRejected", redriveRejected.get());
        m.put("reEnqueued", reEnqueued.get());
        return m;
    }
}
