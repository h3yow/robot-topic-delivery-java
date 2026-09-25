package com.github.h3yow.robottopicdeliveryjava.messaging.exception;

/** 负载编码（序列化）失败。 */
public class EncodingFailureException extends MessagingException {
    public EncodingFailureException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
