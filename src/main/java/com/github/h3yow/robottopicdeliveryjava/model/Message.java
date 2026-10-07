package com.github.h3yow.robottopicdeliveryjava.model;

import com.github.h3yow.robottopicdeliveryjava.topic.Topic;

import java.time.Instant;
import java.util.UUID;

/**
 * 一条已编码、待投递的消息。
 *
 * @param id          本次投递生命周期内的唯一标识；重投会生成全新的 id（全新投递，不复活旧记录）
 * @param originId    最初那条消息的 id：普通发布等于 id；死信重投时沿用其原始链路 id，便于追溯
 */
public record Message(String id, String originId, Topic topic, byte[] payload, Instant createdAt) {

    public static Message of(Topic topic, byte[] payload) {
        String id = UUID.randomUUID().toString();
        return new Message(id, id, topic, payload, Instant.now());
    }

    /**
     * 以重投方式重建一条全新消息：获得新的 {@code id}，但保留原始链路的 {@code originId}。
     */
    public static Message redelivered(Topic topic, byte[] payload, String originId) {
        return new Message(UUID.randomUUID().toString(), originId, topic, payload, Instant.now());
    }
}
