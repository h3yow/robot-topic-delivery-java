package com.github.h3yow.robottopicdeliveryjava.messaging.exception;

/** 负载为空或与主题声明的负载类型不匹配。 */
public class InvalidPayloadException extends MessagingException {
    public InvalidPayloadException(String reason) {
        super(reason);
    }
}
