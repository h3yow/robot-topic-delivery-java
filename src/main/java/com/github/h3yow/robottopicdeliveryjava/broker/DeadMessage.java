package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.topic.Topic;

import java.time.Instant;
import java.util.Optional;

/**
 * 一条重试耗尽后被完整留存的死信记录（快照、不可变）。
 *
 * @param recordId         留存记录自身的标识
 * @param messageId        原始消息标识（已终结投递的标识，不会被复活）
 * @param originMessageId  若本记录由重投再次终结产生，指向最初那条消息；否则等于 messageId
 * @param subscriptionId   原本属于哪个订阅
 * @param topic            原始主题
 * @param payload          原始负载（JSON 字节的防御性拷贝）
 * @param attempts         实际投递过的次数（1 + 重试次数）
 * @param terminatedAt     什么时候终结的
 * @param status           留存处置状态（RETAINED / REDRIVEN）
 * @param redeliveredAsId  若已重投，重投产生的新消息标识
 */
public record DeadMessage(
        String recordId,
        String messageId,
        String originMessageId,
        String subscriptionId,
        Topic topic,
        byte[] payload,
        int attempts,
        Instant terminatedAt,
        DeadMessageStatus status,
        String redeliveredAsId) {

    public DeadMessage {
        payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public boolean retained() {
        return status == DeadMessageStatus.RETAINED;
    }

    public Optional<String> redeliveredMessageId() {
        return Optional.ofNullable(redeliveredAsId);
    }
}
