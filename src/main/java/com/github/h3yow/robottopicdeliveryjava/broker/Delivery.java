package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.model.Message;

/**
 * 一次具体投递的句柄，交给订阅者回调使用。
 * ack() 幂等：重复确认、迟到确认均被安全处理并计入统计。
 */
public interface Delivery {
    Message message();

    /** 第几次投递（从 1 开始，重试递增） */
    int attempt();

    /** 确认本次投递。重复/迟到确认不会改变终态，只计入统计。 */
    void ack();
}
