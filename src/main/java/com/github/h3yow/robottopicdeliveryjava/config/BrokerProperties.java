package com.github.h3yow.robottopicdeliveryjava.config;

import com.github.h3yow.robottopicdeliveryjava.broker.OverflowPolicy;
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

    /** 失败留存（死信）最大条数，超限时按 overflow-policy 取舍 */
    private int deadLetterCapacity = 1000;

    /** 留存容量超限策略：DROP_OLDEST（淘汰最老）或 DROP_NEWEST（拒绝新记录） */
    private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_OLDEST;

    /** 首次重试前的退避等待（毫秒），0 表示立即重试（保持旧行为） */
    private long retryBackoffInitialMillis = 0;

    /** 逐次重试退避倍率，1.0 表示固定间隔 */
    private double retryBackoffMultiplier = 1.0d;

    /** 退避等待上限（毫秒），0 表示不设上限 */
    private long retryBackoffMaxMillis = 0;

    public int getQueueCapacityPerSubscription() {
        return queueCapacityPerSubscription; }
    public void setQueueCapacityPerSubscription(int v) {   this.queueCapacityPerSubscription = v; }
    public long getAckTimeoutMillis() {                    return ackTimeoutMillis; }
    public void setAckTimeoutMillis(long v) {              this.ackTimeoutMillis = v; }
    public int getMaxRetries() {                           return maxRetries; }
    public void setMaxRetries(int v) {                     this.maxRetries = v; }
    public long getDispatchIntervalMillis() {              return dispatchIntervalMillis; }
    public void setDispatchIntervalMillis(long v) {        this.dispatchIntervalMillis = v; }
    public int getDeadLetterCapacity() {                   return deadLetterCapacity; }
    public void setDeadLetterCapacity(int v) {             this.deadLetterCapacity = v; }
    public OverflowPolicy getOverflowPolicy() {            return overflowPolicy; }
    public void setOverflowPolicy(OverflowPolicy v) {      this.overflowPolicy = v; }
    public long getRetryBackoffInitialMillis() {           return retryBackoffInitialMillis; }
    public void setRetryBackoffInitialMillis(long v) {     this.retryBackoffInitialMillis = v; }
    public double getRetryBackoffMultiplier() {            return retryBackoffMultiplier; }
    public void setRetryBackoffMultiplier(double v) {      this.retryBackoffMultiplier = v; }
    public long getRetryBackoffMaxMillis() {               return retryBackoffMaxMillis; }
    public void setRetryBackoffMaxMillis(long v) {         this.retryBackoffMaxMillis = v; }

    public BrokerConfig toConfig() {
        return new BrokerConfig(queueCapacityPerSubscription, ackTimeoutMillis,
                maxRetries, dispatchIntervalMillis, deadLetterCapacity, overflowPolicy,
                retryBackoffInitialMillis, retryBackoffMultiplier, retryBackoffMaxMillis);
    }
}
