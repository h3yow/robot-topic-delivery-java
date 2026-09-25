package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.Objects;

/** 一条已被 Broker 接受的不可变消息（含编码后的字节，供恢复后补投而无需重新编码）。 */
public final class PublishedMessage {

    private final String messageId;
    private final String topic;
    private final Object payload;
    private final byte[] encodedPayload;
    private final long publishNanos;

    public PublishedMessage(String messageId, String topic, Object payload,
                            byte[] encodedPayload, long publishNanos) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.payload = payload;
        this.encodedPayload = Objects.requireNonNull(encodedPayload, "encodedPayload");
        this.publishNanos = publishNanos;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }

    public Object getPayload() {
        return payload;
    }

    public byte[] getEncodedPayload() {
        return encodedPayload;
    }

    public long getPublishNanos() {
        return publishNanos;
    }
}
