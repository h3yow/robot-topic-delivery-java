package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker 全局可核验统计。所有计数器只增不减；
 * 不变式：enqueued = acked + dead + pendingTotal + inflightTotal，
 * 其中 pendingTotal/inflightTotal 来自 {@link BrokerSnapshot#subscriptions()} 之外的记录视图。
 */
public final class BrokerStats {

    private final AtomicLong publishedAccepted = new AtomicLong();
    private final AtomicLong publishedRejected = new AtomicLong();
    private final AtomicLong publishedNoSubscriber = new AtomicLong();
    private final AtomicLong deliveriesEnqueued = new AtomicLong();
    private final AtomicLong deliveriesAcked = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong deadDeliveries = new AtomicLong();
    private final AtomicLong duplicateAcks = new AtomicLong();
    private final AtomicLong lateAcks = new AtomicLong();
    private final AtomicLong rejectedEnqueues = new AtomicLong();
    private final Map<DeadReason, AtomicLong> deadByReason = new EnumMap<>(DeadReason.class);

    public BrokerStats() {
        for (DeadReason reason : DeadReason.values()) {
            deadByReason.put(reason, new AtomicLong());
        }
    }

    public void incrementPublishedAccepted() {
        publishedAccepted.incrementAndGet();
    }

    public void incrementPublishedRejected() {
        publishedRejected.incrementAndGet();
    }

    public void incrementPublishedNoSubscriber() {
        publishedNoSubscriber.incrementAndGet();
    }

    public void incrementDeliveriesEnqueued() {
        deliveriesEnqueued.incrementAndGet();
    }

    public void incrementDeliveriesAcked() {
        deliveriesAcked.incrementAndGet();
    }

    public void incrementRetries() {
        retries.incrementAndGet();
    }

    public void incrementDead(DeadReason reason) {
        deadDeliveries.incrementAndGet();
        deadByReason.get(reason).incrementAndGet();
    }

    public void incrementDuplicateAcks() {
        duplicateAcks.incrementAndGet();
    }

    public void incrementLateAcks() {
        lateAcks.incrementAndGet();
    }

    public void incrementRejectedEnqueues() {
        rejectedEnqueues.incrementAndGet();
    }

    public long getPublishedAccepted() {
        return publishedAccepted.get();
    }

    public long getPublishedRejected() {
        return publishedRejected.get();
    }

    public long getPublishedNoSubscriber() {
        return publishedNoSubscriber.get();
    }

    public long getDeliveriesEnqueued() {
        return deliveriesEnqueued.get();
    }

    public long getDeliveriesAcked() {
        return deliveriesAcked.get();
    }

    public long getRetries() {
        return retries.get();
    }

    public long getDeadDeliveries() {
        return deadDeliveries.get();
    }

    public long getDuplicateAcks() {
        return duplicateAcks.get();
    }

    public long getLateAcks() {
        return lateAcks.get();
    }

    public long getRejectedEnqueues() {
        return rejectedEnqueues.get();
    }

    public Map<DeadReason, Long> getDeadByReason() {
        Map<DeadReason, Long> snapshot = new LinkedHashMap<>();
        for (DeadReason reason : DeadReason.values()) {
            long value = deadByReason.get(reason).get();
            if (value > 0) {
                snapshot.put(reason, value);
            }
        }
        return snapshot;
    }
}
