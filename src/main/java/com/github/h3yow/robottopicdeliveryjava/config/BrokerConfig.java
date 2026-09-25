package com.github.h3yow.robottopicdeliveryjava.config;

/**
 *  broker 运行参数。
 *
 * @param queueCapacityPerSubscription 每个订阅的待投递队列容量，满时按背压策略拒绝
 * @param ackTimeoutMillis             投递后等待确认的超时时间，超时触发重试
 * @param maxRetries                   单条消息最大重试次数，超过后进入终止（DEAD）状态
 * @param dispatchIntervalMillis       派发循环的空转间隔
 */
public record BrokerConfig(
        int queueCapacityPerSubscription,
        long ackTimeoutMillis,
        int maxRetries,
        long dispatchIntervalMillis) {

    public static BrokerConfig defaults() {
        return new BrokerConfig(64, 500, 3, 5);
    }
}
