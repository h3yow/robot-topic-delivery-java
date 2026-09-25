package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.topic.TopicFilter;

/**
 * 订阅句柄，标识一次订阅注册。
 */
public record Subscription(String id, TopicFilter filter) {
}
