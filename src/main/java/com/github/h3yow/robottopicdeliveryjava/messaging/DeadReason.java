package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 投递进入 DEAD 终态的可区分原因。 */
public enum DeadReason {
    /** 重试次数达到上限仍未确认。 */
    RETRIES_EXHAUSTED,
    /** 订阅被动态移除，消息不再有承诺对象。 */
    SUBSCRIBER_REMOVED,
    /** DROP_OLDEST 背压策略下，最旧的待投递消息被淘汰。 */
    DROPPED_OLDEST,
    /** REJECT 背压策略下，队列满，消息被拒绝。 */
    QUEUE_FULL_REJECTED,
    /** 消息编码失败，无法投递。 */
    ENCODING_FAILURE,
    /** 订阅断开后未在宽限期内恢复。 */
    CONNECTION_LOST
}
