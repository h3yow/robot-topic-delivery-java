package com.github.h3yow.robottopicdeliveryjava.messaging.exception;

/** 主题或过滤规则语法非法。 */
public class InvalidTopicException extends MessagingException {
    public InvalidTopicException(String reason) {
        super(reason);
    }
}
