package com.github.h3yow.robottopicdeliveryjava.topic;

/**
 * 主题名或主题过滤器非法时抛出。
 */
public class InvalidTopicException extends RuntimeException {
    public InvalidTopicException(String message) {
        super(message);
    }
}
