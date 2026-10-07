package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 失败留存容量超限时的取舍策略。
 */
public enum OverflowPolicy {
    /** 淘汰最老的留存（按终结时间最早），新记录得以保留 */
    DROP_OLDEST,
    /** 拒绝保留新记录（新终结的消息不留存），老记录保留 */
    DROP_NEWEST
}
