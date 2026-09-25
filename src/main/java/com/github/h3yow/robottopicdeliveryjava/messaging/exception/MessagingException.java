package com.github.h3yow.robottopicdeliveryjava.messaging.exception;

/** 消息系统所有显式拒绝错误的基类，携带可区分的原因。 */
public class MessagingException extends RuntimeException {

    private final String reason;

    public MessagingException(String reason) {
        super(reason);
        this.reason = reason;
    }

    public MessagingException(String reason, Throwable cause) {
        super(reason, cause);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
