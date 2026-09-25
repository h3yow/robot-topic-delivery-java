package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 单条消息发布的判定结果。 */
public enum PublishStatus {
    /** 已被接受并完成入队（每个匹配订阅者均成功）。 */
    ACCEPTED,
    /** 被拒绝（未知主题/非法负载/编码失败/队列拒绝等）。 */
    REJECTED
}
