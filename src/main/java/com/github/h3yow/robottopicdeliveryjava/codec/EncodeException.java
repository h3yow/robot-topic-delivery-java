package com.github.h3yow.robottopicdeliveryjava.codec;

/**
 * 负载编码（序列化为 JSON 字节）失败时抛出。
 */
public class EncodeException extends RuntimeException {
    public EncodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
