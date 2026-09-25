package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 单个订阅的诊断快照。 */
public record SubscriptionSnapshot(
        String subscriberId,
        String filter,
        boolean connected,
        int queueSize,
        int queueCapacity,
        long enqueued,
        long acked,
        long retried,
        long dead,
        long duplicateAcks,
        long rejectedEnqueues
) {
}
