package com.github.h3yow.robottopicdeliveryjava.config;

import com.github.h3yow.robottopicdeliveryjava.broker.OverflowPolicy;

/**
 * broker 运行参数。
 *
 * @param queueCapacityPerSubscription 每个订阅的待投递队列容量，满时按背压策略拒绝
 * @param ackTimeoutMillis             投递后等待确认的超时时间，超时触发重试
 * @param maxRetries                   单条消息最大重试次数，超过后进入终止（DEAD）状态
 * @param dispatchIntervalMillis       派发循环的空转间隔
 * @param deadLetterCapacity           失败留存（死信）最大条数
 * @param overflowPolicy               留存容量超限时的取舍策略
 * @param retryBackoffInitialMillis    首次重试前的退避等待（毫秒）；0 表示立即重试（兼容旧行为）
 * @param retryBackoffMultiplier       逐次重试退避倍率（1.0 表示固定间隔）
 * @param retryBackoffMaxMillis        退避等待上限（毫秒）；0 表示不设上限
 */
public record BrokerConfig(
        int queueCapacityPerSubscription,
        long ackTimeoutMillis,
        int maxRetries,
        long dispatchIntervalMillis,
        int deadLetterCapacity,
        OverflowPolicy overflowPolicy,
        long retryBackoffInitialMillis,
        double retryBackoffMultiplier,
        long retryBackoffMaxMillis) {

    /** 规范化与校验：容量/超时/重试/退避参数必须落在合法区间，非法配置在启动时快速失败。 */
    public BrokerConfig {
        if (queueCapacityPerSubscription <= 0) {
            throw new IllegalArgumentException(
                    "queueCapacityPerSubscription must be positive: " + queueCapacityPerSubscription);
        }
        if (ackTimeoutMillis <= 0) {
            throw new IllegalArgumentException("ackTimeoutMillis must be positive: " + ackTimeoutMillis);
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative: " + maxRetries);
        }
        if (dispatchIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "dispatchIntervalMillis must be positive: " + dispatchIntervalMillis);
        }
        if (deadLetterCapacity <= 0) {
            throw new IllegalArgumentException(
                    "deadLetterCapacity must be positive: " + deadLetterCapacity);
        }
        if (overflowPolicy == null) {
            throw new IllegalArgumentException("overflowPolicy must not be null");
        }
        if (retryBackoffInitialMillis < 0 || retryBackoffMaxMillis < 0) {
            throw new IllegalArgumentException("retry backoff millis must not be negative");
        }
        if (retryBackoffMultiplier < 1.0d || Double.isNaN(retryBackoffMultiplier)) {
            throw new IllegalArgumentException(
                    "retryBackoffMultiplier must be >= 1.0: " + retryBackoffMultiplier);
        }
    }

    /** 兼容旧调用的 4 参数构造：退避为 0（立即重试），留存容量 1000、淘汰最老。 */
    public BrokerConfig(int queueCapacityPerSubscription, long ackTimeoutMillis,
                        int maxRetries, long dispatchIntervalMillis) {
        this(queueCapacityPerSubscription, ackTimeoutMillis, maxRetries,
                dispatchIntervalMillis, 1000, OverflowPolicy.DROP_OLDEST, 0L, 1.0d, 0L);
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(64, 500, 3, 5, 1000, OverflowPolicy.DROP_OLDEST, 0L, 2.0d, 30_000L);
    }
}
