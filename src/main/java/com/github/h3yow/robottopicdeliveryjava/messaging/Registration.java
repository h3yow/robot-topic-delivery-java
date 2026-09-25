package com.github.h3yow.robottopicdeliveryjava.messaging;

/** 主题注册结果。 */
public record Registration(RegistrationStatus status, String topic, String reason) {
}
