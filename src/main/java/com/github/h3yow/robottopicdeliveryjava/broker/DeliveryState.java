package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 单条投递（消息 x 订阅）的状态机：
 * PENDING -> IN_FLIGHT -> ACKED（正常确认）
 * IN_FLIGHT -> PENDING（超时重试，回到队列）
 * IN_FLIGHT -> DEAD（重试耗尽，终止）
 * 终态（ACKED/DEAD）不可逆；重复或迟到确认不改变终态。
 */
public enum DeliveryState {
    PENDING,
    IN_FLIGHT,
    ACKED,
    DEAD
}
