package com.github.h3yow.robottopicdeliveryjava.messaging.exception;

/** 发布到未注册（未知）主题。 */
public class UnknownTopicException extends MessagingException {
    public UnknownTopicException(String reason) {
        super(reason);
    }
}
