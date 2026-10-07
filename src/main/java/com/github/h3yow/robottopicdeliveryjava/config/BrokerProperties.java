package com.github.h3yow.robottopicdeliveryjava.config;

import com.github.h3yow.robottopicdeliveryjava.broker.DeadLetterOverflowPolicy;
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

    /** 首次重试前的退避等待（毫秒）；0 表示立即重试（旧行为） */
    private long retryBackoffInitialMillis = 100;

    /** 退避倍数，逐次变长：第 n 次重试等待 initial * multiplier^(n-1) */
    private double retryBackoffMultiplier = 2.0;

    /** 单次退避等待上限（毫秒） */
    private long retryBackoffMaxMillis = 5000;

    /** 死信留存条数上限（全 broker） */
    private int deadLetterCapacity = 256;

    /** 死信留存超限时的取舍策略：EVICT_OLDEST（淘汰最旧）或 REJECT_NEW（拒绝新记录） */
    private DeadLetterOverflowPolicy deadLetterOverflowPolicy = DeadLetterOverflowPolicy.EVICT_OLDEST;

    public int getQueueCapacityPerSubscription() {
        return queueCapacityPerSubscription; }
    public void setQueueCapacityPerSubscription(int v) {   this.queueCapacityPerSubscription = v; }
    public long getAckTimeoutMillis() {                    return ackTimeoutMillis; }
    public void setAckTimeoutMillis(long v) {              this.ackTimeoutMillis = v; }
    public int getMaxRetries() {                           return maxRetries; }
    public void setMaxRetries(int v) {                     this.maxRetries = v; }
    public long getDispatchIntervalMillis() {              return dispatchIntervalMillis; }
    public void setDispatchIntervalMillis(long v) {        this.dispatchIntervalMillis = v; }
    public long getRetryBackoffInitialMillis() {           return retryBackoffInitialMillis; }
    public void setRetryBackoffInitialMillis(long v) {     this.retryBackoffInitialMillis = v; }
    public double getRetryBackoffMultiplier() {            return retryBackoffMultiplier; }
    public void setRetryBackoffMultiplier(double v) {      this.retryBackoffMultiplier = v; }
    public long getRetryBackoffMaxMillis() {               return retryBackoffMaxMillis; }
    public void setRetryBackoffMaxMillis(long v) {         this.retryBackoffMaxMillis = v; }
    public int getDeadLetterCapacity() {                   return deadLetterCapacity; }
    public void setDeadLetterCapacity(int v) {             this.deadLetterCapacity = v; }
    public DeadLetterOverflowPolicy getDeadLetterOverflowPolicy() { return deadLetterOverflowPolicy; }
    public void setDeadLetterOverflowPolicy(DeadLetterOverflowPolicy v) {
        this.deadLetterOverflowPolicy = v;
    }

    public BrokerConfig toConfig() {
        return new BrokerConfig(queueCapacityPerSubscription, ackTimeoutMillis,
                maxRetries, dispatchIntervalMillis, retryBackoffInitialMillis,
                retryBackoffMultiplier, retryBackoffMaxMillis, deadLetterCapacity,
                deadLetterOverflowPolicy);
    }
}
