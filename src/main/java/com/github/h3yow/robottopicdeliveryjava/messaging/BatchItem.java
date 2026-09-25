package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 批量发布中的单条输入。 */
public record BatchItem(String topic, Object payload) {
}
