package com.github.h3yow.robottopicdeliveryjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * broker 配置项（application.properties 中以 broker.* 前缀配置）。
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 每个订阅的待投递队列容量，满时背压拒绝 */
    private int queueCapacityPerSubscription = 64;

    /** 投递后等待确认的超时时间（毫秒），超时触发重试 */
    private long ackTimeoutMillis = 500;

    /** 单条消息最大重试次数，超过后进入 DEAD 终态 */
    private int maxRetries = 3;

    /** 派发循环空转间隔（毫秒） */
    private long dispatchIntervalMillis = 5;

    public int getQueueCapacityPerSubscription() {
        return queueCapacityPerSubscription; }
    public void setQueueCapacityPerSubscription(int v) {   this.queueCapacityPerSubscription = v; }
    public long getAckTimeoutMillis() {                    return ackTimeoutMillis; }
    public void setAckTimeoutMillis(long v) {              this.ackTimeoutMillis = v; }
    public int getMaxRetries() {                           return maxRetries; }
    public void setMaxRetries(int v) {                     this.maxRetries = v; }
    public long getDispatchIntervalMillis() {              return dispatchIntervalMillis; }
    public void setDispatchIntervalMillis(long v) {        this.dispatchIntervalMillis = v; }

    public BrokerConfig toConfig() {
        return new BrokerConfig(queueCapacityPerSubscription, ackTimeoutMillis,
                maxRetries, dispatchIntervalMillis);
    }
}
