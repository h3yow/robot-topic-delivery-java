package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 单条投递（消息 x 订阅）的状态机：
 * PENDING -> IN_FLIGHT -> ACKED（正常确认）
 * IN_FLIGHT -> BACKOFF（ack 超时，进入逐次变长的退避等待）
 * BACKOFF  -> PENDING -> IN_FLIGHT（退避结束后重试）
 * IN_FLIGHT -> DEAD（重试耗尽，终止并留存死信）
 * 终态（ACKED/DEAD）不可逆；重复或迟到确认不改变终态。
 * 退避期间断连只暂停派发、不改变退避；退避结束后若仍断连则等待恢复，再重试。
 */
public enum DeliveryState {
    PENDING,
    IN_FLIGHT,
    BACKOFF,
    ACKED,
    DEAD
}
