package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 单条消息发布的回执：成功携带消息 ID，拒绝携带可区分原因。 */
public record PublishReceipt(PublishStatus status, String messageId, String reason) {

    public boolean accepted() {
        return status == PublishStatus.ACCEPTED;
    }
}
