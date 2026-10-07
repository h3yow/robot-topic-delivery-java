package com.github.h3yow.robottopicdeliveryjava.config;

import com.github.h3yow.robottopicdeliveryjava.broker.DeadLetterOverflowPolicy;

/**
 * broker 运行参数。
 *
 * @param queueCapacityPerSubscription 每个订阅的待投递队列容量，满时按背压策略拒绝
 * @param ackTimeoutMillis             投递后等待确认的超时时间，超时触发重试
 * @param maxRetries                   单条消息最大重试次数，超过后进入终止（DEAD）状态
 * @param dispatchIntervalMillis       派发循环的空转间隔
 * @param retryBackoffInitialMillis    第 1 次重试前的初始退避等待；0 表示立即重试（兼容旧行为）
 * @param retryBackoffMultiplier       退避倍数，第 n 次重试（n 从 1 起）等待
 *                                     initial * multiplier^(n-1)；1.0 表示固定间隔
 * @param retryBackoffMaxMillis        单次退避等待上限
 * @param deadLetterCapacity           死信留存条数上限（全 broker）
 * @param deadLetterOverflowPolicy     留存超限时的取舍策略
 */
public record BrokerConfig(
        int queueCapacityPerSubscription,
        long ackTimeoutMillis,
        int maxRetries,
        long dispatchIntervalMillis,
        long retryBackoffInitialMillis,
        double retryBackoffMultiplier,
        long retryBackoffMaxMillis,
        int deadLetterCapacity,
        DeadLetterOverflowPolicy deadLetterOverflowPolicy) {

    /**
     * 兼容旧构造：退避为 0（立即重试，与历史行为完全一致），死信容量 256、超限淘汰最旧。
     */
    public BrokerConfig(int queueCapacityPerSubscription, long ackTimeoutMillis,
                        int maxRetries, long dispatchIntervalMillis) {
        this(queueCapacityPerSubscription, ackTimeoutMillis, maxRetries, dispatchIntervalMillis,
                0L, 1.0, 0L, 256, DeadLetterOverflowPolicy.EVICT_OLDEST);
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(64, 500, 3, 5,
                100L, 2.0, 5_000L, 256, DeadLetterOverflowPolicy.EVICT_OLDEST);
    }

    public BrokerConfig {
        if (queueCapacityPerSubscription <= 0) {
            throw new IllegalArgumentException("queueCapacityPerSubscription must be positive");
        }
        if (ackTimeoutMillis <= 0) {
            throw new IllegalArgumentException("ackTimeoutMillis must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        if (dispatchIntervalMillis < 0) {
            throw new IllegalArgumentException("dispatchIntervalMillis must not be negative");
        }
        if (retryBackoffInitialMillis < 0) {
            throw new IllegalArgumentException("retryBackoffInitialMillis must not be negative");
        }
        if (Double.isNaN(retryBackoffMultiplier) || retryBackoffMultiplier < 1.0) {
            throw new IllegalArgumentException("retryBackoffMultiplier must be >= 1.0");
        }
        if (retryBackoffMaxMillis < 0) {
            throw new IllegalArgumentException("retryBackoffMaxMillis must not be negative");
        }
        if (deadLetterCapacity <= 0) {
            throw new IllegalArgumentException("deadLetterCapacity must be positive");
        }
        if (deadLetterOverflowPolicy == null) {
            throw new IllegalArgumentException("deadLetterOverflowPolicy must not be null");
        }
    }

    /**
     * 第 retryNumber 次重试前应等待的退避时长（毫秒）。retryNumber 从 1 开始。
     * 结果受 {@code retryBackoffMaxMillis} 钳制（initial 为 0 时恒为 0）。
     */
    public long backoffMillisForRetry(int retryNumber) {
        if (retryBackoffInitialMillis <= 0) {
            return 0L;
        }
        double raw = retryBackoffInitialMillis * Math.pow(retryBackoffMultiplier, retryNumber - 1);
        if (raw >= retryBackoffMaxMillis || Double.isInfinite(raw)) {
            return retryBackoffMaxMillis;
        }
        return (long) raw;
    }
}
