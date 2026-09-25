package com.github.h3yow.robottopicdeliveryjava.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 消息 Broker 配置项，前缀 {@code robot.messaging}。 */
@ConfigurationProperties(prefix = "robot.messaging")
public class BrokerProperties {

    private int queueCapacity = 128;
    private BackpressurePolicy backpressurePolicy = BackpressurePolicy.BLOCK;
    private long ackTimeoutMillis = 5_000L;
    private int maxAttempts = 5;
    private long disconnectGraceMillis = 30_000L;
    private long timeoutScanIntervalMillis = 20L;

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public BackpressurePolicy getBackpressurePolicy() {
        return backpressurePolicy;
    }

    public void setBackpressurePolicy(BackpressurePolicy backpressurePolicy) {
        this.backpressurePolicy = backpressurePolicy;
    }

    public long getAckTimeoutMillis() {
        return ackTimeoutMillis;
    }

    public void setAckTimeoutMillis(long ackTimeoutMillis) {
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getDisconnectGraceMillis() {
        return disconnectGraceMillis;
    }

    public void setDisconnectGraceMillis(long disconnectGraceMillis) {
        this.disconnectGraceMillis = disconnectGraceMillis;
    }

    public long getTimeoutScanIntervalMillis() {
        return timeoutScanIntervalMillis;
    }

    public void setTimeoutScanIntervalMillis(long timeoutScanIntervalMillis) {
        this.timeoutScanIntervalMillis = timeoutScanIntervalMillis;
    }
}
