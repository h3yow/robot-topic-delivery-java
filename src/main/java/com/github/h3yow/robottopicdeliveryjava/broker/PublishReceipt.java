package com.github.h3yow.robottopicdeliveryjava.broker;

import java.util.List;

/**
 * 一次发布的结果。要么整体被接受（含每个订阅的入队结果），
 * 要么因主题/负载/编码原因被整体拒绝。
 */
public record PublishReceipt(
        boolean accepted,
        String messageId,
        RejectReason rejectReason,
        List<EnqueueResult> enqueueResults) {

    public static PublishReceipt rejected(RejectReason reason) {
        return new PublishReceipt(false, null, reason, List.of());
    }

    public static PublishReceipt accepted(String messageId, List<EnqueueResult> results) {
        return new PublishReceipt(true, messageId, null, List.copyOf(results));
    }

    /**
     * @param subscriptionId 订阅标识
     * @param enqueued       是否成功入队；false 表示该订阅队列满被背压拒绝（不影响其他订阅）
     */
    public record EnqueueResult(String subscriptionId, boolean enqueued) {
    }
}
