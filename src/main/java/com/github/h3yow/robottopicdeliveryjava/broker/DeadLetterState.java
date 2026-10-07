package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 失败留存记录的处置状态（留存记录自身的生命周期，与 {@link DeliveryState} 独立）：
 * RETAINED  -> REPLAYING （重投请求已接受，新投递已入队/在途）
 * RETAINED  -> REPLAYED  （重投产生的新投递已终结）
 * REPLAYING -> REPLAYED
 * 已进入 REPLAYING/REPLAYED 的记录不可再次重投。
 */
public enum DeadLetterState {
    RETAINED,
    REPLAYING,
    REPLAYED
}
