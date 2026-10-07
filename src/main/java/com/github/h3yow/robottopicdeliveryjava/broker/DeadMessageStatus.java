package com.github.h3yow.robottopicdeliveryjava.broker;

/**
 * 留存死信的处置状态：
 * RETAINED —— 仍留存、可被重投；
 * REDRIVEN —— 已作为一次全新投递重新入队，原记录不再可重投（不复活、不重复生效）。
 */
public enum DeadMessageStatus {
    RETAINED,
    REDRIVEN
}
