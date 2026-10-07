package com.github.h3yow.robottopicdeliveryjava.model;

import com.github.h3yow.robottopicdeliveryjava.topic.Topic;

import java.time.Instant;
import java.util.UUID;

/**
 * 一条已编码、待投递的消息。
 *
 * @param originDeadLetterId 若由失败留存重投产生，指向上一次失败留存的记录 id；正常发布为 null
 */
public record Message(String id, Topic topic, byte[] payload, Instant createdAt,
                      String originDeadLetterId) {

    public static Message of(Topic topic, byte[] payload) {
        return new Message(UUID.randomUUID().toString(), topic, payload, Instant.now(), null);
    }

    /** 基于留存记录重投：生成全新 id，主题与负载原样复制。 */
    public static Message replayed(Topic topic, byte[] payload, String originDeadLetterId) {
        return new Message(UUID.randomUUID().toString(), topic, payload, Instant.now(),
                originDeadLetterId);
    }
}
