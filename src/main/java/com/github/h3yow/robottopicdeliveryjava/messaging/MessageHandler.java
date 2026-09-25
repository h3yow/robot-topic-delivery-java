package com.github.h3yow.robottopicdeliveryjava.messaging;

/**
 * 订阅者的消息处理器。处理器正常返回即视为“投递成功、等待确认”；
 * 处理器必须在方法内部通过 {@link DeliveryControl#ack()} / {@link DeliveryControl#nack()} 显式确认。
 * 抛出异常等同于 nack，会触发重试。
 */
@FunctionalInterface
public interface MessageHandler {

    void handle(DeliveredMessage message, DeliveryControl control) throws Exception;

    /** 投递给处理器的消息视图。 */
    interface DeliveredMessage {
        String messageId();
        String topic();
        Object payload();
        int attempt();
    }

    /** 处理器可使用的确认控制句柄。 */
    interface DeliveryControl {
        /** 确认当前投递；重复/乱序确认将被拒绝且幂等。 */
        boolean ack();

        /** 否定确认，立即安排重试（仍计入一次尝试）。 */
        boolean nack();
    }
}
