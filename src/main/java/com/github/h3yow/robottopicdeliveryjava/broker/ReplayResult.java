package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 一次重投请求的确定性结果。
 * accepted=true 时 newMessageId 为这次全新投递对应的消息 id；
 * accepted=false 时 rejectReason 给出可区分的挡下原因。
 */
public record ReplayResult(boolean accepted, String deadLetterId, String newMessageId,
                           ReplayRejectReason rejectReason) {

    static ReplayResult accepted(String deadLetterId, String newMessageId) {
        return new ReplayResult(true, deadLetterId, newMessageId, null);
    }

    static ReplayResult rejected(String deadLetterId, ReplayRejectReason reason) {
        return new ReplayResult(false, deadLetterId, null, reason);
    }
}
