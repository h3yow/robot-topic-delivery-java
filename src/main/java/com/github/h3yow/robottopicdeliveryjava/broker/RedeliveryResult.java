package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 一次死信重投请求的结果。
 * 成功时携带重投产生的全新消息标识（newMessageId）；失败时携带可区分的 {@link RedeliveryRejectReason}。
 */
public record RedeliveryResult(
        boolean accepted,
        String recordId,
        String newMessageId,
        RedeliveryRejectReason rejectReason) {

    static RedeliveryResult accepted(String recordId, String newMessageId) {
        return new RedeliveryResult(true, recordId, newMessageId, null);
    }

    static RedeliveryResult rejected(String recordId, RedeliveryRejectReason reason) {
        return new RedeliveryResult(false, recordId, null, reason);
    }
}
