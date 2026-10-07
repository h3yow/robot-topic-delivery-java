package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.model.Message;

import java.time.Instant;

/**
 * 重试耗尽被终止（DEAD）的消息留存快照。
 *
 * <p>不可变；{@code id} 为本次失败投递对应的消息 id（每个匹配订阅一条独立记录）。
 * {@code state} 描述留存记录的重投处置状态，见 {@link DeadLetterState}。
 */
public record DeadLetter(
        String id,
        String subscriptionId,
        String topic,
        byte[] payload,
        int attempts,
        Instant diedAt,
        DeadLetterState state,
        /** 若由重投产生，指向上一次失败的留存 id；否则为 null */
        String originId,
        /** 已接受的重投所产生的新消息 id；未重投时为 null */
        String replayedAsMessageId) {

    /** 留存（未重投）状态的构造便捷方法。 */
    static DeadLetter retained(Message message, String subscriptionId, int attempts,
                               Instant diedAt, String originId) {
        return new DeadLetter(message.id(), subscriptionId, message.topic().name(),
                message.payload(), attempts, diedAt, DeadLetterState.RETAINED, originId, null);
    }
}
