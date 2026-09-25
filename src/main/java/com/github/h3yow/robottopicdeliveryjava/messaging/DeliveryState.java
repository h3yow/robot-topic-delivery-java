package com.github.h3yow.robottopicdeliveryjava.messaging;

/**
 * 单条投递记录的状态机：
 * PENDING -> IN_FLIGHT -> ACKED / DEAD；IN_FLIGHT -> PENDING（超时重试）。
 * 状态只能沿合法迁移前进，不允许状态倒退。
 */
public enum DeliveryState {
    /** 在订阅者待投递队列中等待。 */
    PENDING,
    /** 已发送给订阅者，等待确认。 */
    IN_FLIGHT,
    /** 已收到订阅者确认，终态。 */
    ACKED,
    /** 终止（重试耗尽、订阅者被移除、队列淘汰、编码失败等），终态。 */
    DEAD
}
