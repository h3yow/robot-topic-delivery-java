package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 发布被拒绝的原因，彼此可区分。
 */
public enum RejectReason {
    /** 主题名非法 */
    INVALID_TOPIC,
    /** 负载为 null 或不符合约束 */
    INVALID_PAYLOAD,
    /** 负载编码失败 */
    ENCODE_FAILURE,
    /** 订阅队列已满（背压拒绝） */
    QUEUE_FULL,
    /** broker 已关闭 */
    BROKER_CLOSED
}
