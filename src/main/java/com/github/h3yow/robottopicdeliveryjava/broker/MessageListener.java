package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 订阅者回调。收到 {@link Delivery} 后应调用其 ack() 确认；
 * 未在超时时间内确认将触发重投。
 */
@FunctionalInterface
public interface MessageListener {
    void onMessage(Delivery delivery);
}
