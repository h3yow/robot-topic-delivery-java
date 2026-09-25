package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.Objects;

/** 订阅的静态定义（每个 subscriberId 唯一；内部投递状态由 Broker 持有）。 */
public final class Subscription {

    private final String subscriberId;
    private final String filter;
    private final MessageHandler handler;
    private final int queueCapacity;
    private final BackpressurePolicy backpressurePolicy;

    public Subscription(String subscriberId, String filter, MessageHandler handler,
                        int queueCapacity, BackpressurePolicy backpressurePolicy) {
        this.subscriberId = Objects.requireNonNull(subscriberId, "subscriberId");
        this.filter = Objects.requireNonNull(filter, "filter");
        this.handler = Objects.requireNonNull(handler, "handler");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive: " + queueCapacity);
        }
        this.queueCapacity = queueCapacity;
        this.backpressurePolicy = Objects.requireNonNull(backpressurePolicy, "backpressurePolicy");
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public String getFilter() {
        return filter;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public BackpressurePolicy getBackpressurePolicy() {
        return backpressurePolicy;
    }
}
