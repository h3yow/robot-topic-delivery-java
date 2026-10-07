package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 重投请求被挡下的原因，彼此可区分。
 */
public enum ReplayRejectReason {
    /** 留存中不存在该 id 的记录（或已被容量溢出淘汰） */
    DEAD_LETTER_NOT_FOUND,
    /** 该记录的重投消息仍在投递路上（REPLAYING），尚未终结 */
    REPLAY_IN_FLIGHT,
    /** 该记录已经重投过且重投消息已终结（REPLAYED），不可再次重投 */
    ALREADY_REPLAYED,
    /** 记录对应的目标订阅已不存在 */
    SUBSCRIPTION_NOT_FOUND,
    /** 重投时目标订阅队列已满（沿用发布背压的确定性拒绝） */
    QUEUE_FULL,
    /** broker 已关闭 */
    BROKER_CLOSED
}
