package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 死信留存容量超限时的取舍策略：
 * EVICT_OLDEST —— 淘汰最早终结留存的记录，新记录留存（默认，记账为 deadEvicted）；
 * REJECT_NEW   —— 保留既有记录，新终结的消息不留存（记账为 deadArchiveRejected）。
 */
public enum DeadLetterOverflowPolicy {
    EVICT_OLDEST,
    REJECT_NEW
}
