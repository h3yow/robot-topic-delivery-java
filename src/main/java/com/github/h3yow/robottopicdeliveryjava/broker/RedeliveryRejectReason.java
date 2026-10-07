package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 死信重投被挡下的原因，彼此可区分。
 */
public enum RedeliveryRejectReason {
    /** broker 已关闭 */
    BROKER_CLOSED,
    /** 目标订阅已不存在（从未注册或已被移除） */
    SUBSCRIPTION_GONE,
    /** 该订阅下不存在由该原始消息终结而来的留存记录（消息从未在该订阅终结） */
    RECORD_NOT_FOUND,
    /** 该留存记录已经重投过，不能再次重投（不复活、不重复生效） */
    ALREADY_REDELIVERED,
    /** 原始投递尚未终结（仍在队列/在途/退避等待中），不属于可重投的留存消息 */
    STILL_IN_FLIGHT,
    /** 重投入队时目标订阅队列已满，沿用既有的确定性背压拒绝（不悄悄吞掉） */
    QUEUE_FULL,
    /** 留存容量按 REJECT_NEW 策略已满，新终结消息未留存，因此无记录可重投 */
    RETENTION_FULL
}
