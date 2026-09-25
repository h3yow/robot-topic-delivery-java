package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.List;

/**
 * 机器人主题通信与可靠投递的核心入口。
 * 实现必须线程安全；语义为“至少一次投递（at-least-once）”，
 * 重复投递时 attempt 严格递增，订阅者需以 messageId 自行去重。
 */
public interface MessageBroker {

    /** 注册一个可发布主题（重复注册同一主题返回 REJECTED 并给出原因）。 */
    Registration registerTopic(String topic, Class<?> payloadType, boolean requirePayload);

    /** 新增订阅；同一 subscriberId 重复订阅返回 REJECTED。 */
    Registration subscribe(String subscriberId, String filter, MessageHandler handler);

    /** 以显式容量与背压策略新增订阅。 */
    Registration subscribe(String subscriberId, String filter, MessageHandler handler,
                           int queueCapacity, BackpressurePolicy policy);

    /** 动态移除订阅；队列中尚未承诺的消息按 SUBSCRIBER_REMOVED 终止并计数。 */
    void unsubscribe(String subscriberId);

    /** 发布单条消息；任何拒绝都通过回执返回，不影响其他消息。 */
    PublishReceipt publish(String topic, Object payload);

    /** 批量发布，逐条隔离失败。 */
    BatchPublishResult publishBatch(List<BatchItem> items);

    /** 声明订阅者连接中断：停止投递，等待在宽限期内恢复。 */
    void disconnect(String subscriberId);

    /** 连接恢复：对所有未确认（含在途中）消息补投，不静默丢失已承诺消息。 */
    void recover(String subscriberId);

    /** 可核验的诊断快照（统计与各订阅队列水位）。 */
    BrokerSnapshot snapshot();

    /** 优雅关闭：终止所有投递线程。 */
    void close();
}
