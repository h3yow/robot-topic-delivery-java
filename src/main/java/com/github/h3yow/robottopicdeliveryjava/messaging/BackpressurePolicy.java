package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 订阅者队列饱和时的确定性策略。 */
public enum BackpressurePolicy {
    /** 阻塞发布方，直到队列有空位。 */
    BLOCK,
    /** 立即拒绝本次入队，计入 rejected 统计与 DEAD 记录。 */
    REJECT,
    /** 淘汰队列头部最旧的一条待投递消息，新消息入队尾部。 */
    DROP_OLDEST
}
