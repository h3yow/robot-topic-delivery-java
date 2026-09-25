package com.github.h3yow.robottopicdeliveryjava.model;

import com.github.h3yow.robottopicdeliveryjava.topic.Topic;

import java.time.Instant;
import java.util.UUID;

/**
 * 一条已编码、待投递的消息。
 */
public record Message(String id, Topic topic, byte[] payload, Instant createdAt) {

    public static Message of(Topic topic, byte[] payload) {
        return new Message(UUID.randomUUID().toString(), topic, payload, Instant.now());
    }
}
