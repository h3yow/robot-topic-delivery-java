package com.github.h3yow.robottopicdeliveryjava.messaging.web;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 仅供本地验证的 HTTP 入口：提供主题注册、发布（单条/批量）、
 * 订阅增删、连接断开/恢复以及诊断快照。订阅处理器用内存收集最近投递，
 * 并对每条消息自动确认（可用 ack=false 模拟永不确认以观察超时重试）。
 */
@RestController
@RequestMapping("/api/broker")
public class BrokerController {

    private final MessageBroker broker;

    /** subscriberId -> 最近收到的投递（本地验证用）。 */
    private final Map<String, CopyOnWriteArrayList<Map<String, Object>>> inbox = new ConcurrentHashMap<>();

    public BrokerController(MessageBroker broker) {
        this.broker = broker;
    }

    public record TopicRequest(String topic, String payloadType, Boolean requirePayload) {
    }

    public record PublishRequest(String topic, Object payload) {
    }

    public record SubscribeRequest(String subscriberId, String filter,
                                   Integer queueCapacity, BackpressurePolicy backpressurePolicy,
                                   Boolean autoAck) {
    }

    @PostMapping("/topics")
    public Registration registerTopic(@RequestBody TopicRequest request) throws ClassNotFoundException {
        Class<?> type = Class.forName(request.payloadType() == null
                ? String.class.getName() : request.payloadType());
        boolean require = request.requirePayload() == null || request.requirePayload();
        return broker.registerTopic(request.topic(), type, require);
    }

    @PostMapping("/publish")
    public PublishReceipt publish(@RequestBody PublishRequest request) {
        return broker.publish(request.topic(), request.payload());
    }

    @PostMapping("/publish-batch")
    public BatchPublishResult publishBatch(@RequestBody List<PublishRequest> requests) {
        return broker.publishBatch(requests.stream()
                .map(r -> new BatchItem(r.topic(), r.payload()))
                .toList());
    }

    @PostMapping("/subscribe")
    public Registration subscribe(@RequestBody SubscribeRequest request) {
        boolean autoAck = request.autoAck() == null || request.autoAck();
        inbox.computeIfAbsent(request.subscriberId(), k -> new CopyOnWriteArrayList<>());
        MessageHandler handler = (message, control) -> {
            CopyOnWriteArrayList<Map<String, Object>> messages =
                    inbox.computeIfAbsent(request.subscriberId(), k -> new CopyOnWriteArrayList<>());
            messages.add(Map.of(
                    "messageId", message.messageId(),
                    "topic", message.topic(),
                    "payload", String.valueOf(message.payload()),
                    "attempt", message.attempt()));
            if (autoAck) {
                control.ack();
            }
        };
        if (request.queueCapacity() != null && request.backpressurePolicy() != null) {
            return broker.subscribe(request.subscriberId(), request.filter(), handler,
                    request.queueCapacity(), request.backpressurePolicy());
        }
        return broker.subscribe(request.subscriberId(), request.filter(), handler);
    }

    @DeleteMapping("/subscribe/{subscriberId}")
    public Map<String, Object> unsubscribe(@PathVariable String subscriberId) {
        broker.unsubscribe(subscriberId);
        return Map.of("status", "REMOVED", "subscriberId", subscriberId);
    }

    @PostMapping("/subscribe/{subscriberId}/disconnect")
    public Map<String, Object> disconnect(@PathVariable String subscriberId) {
        broker.disconnect(subscriberId);
        return Map.of("status", "DISCONNECTED", "subscriberId", subscriberId);
    }

    @PostMapping("/subscribe/{subscriberId}/recover")
    public Map<String, Object> recover(@PathVariable String subscriberId) {
        broker.recover(subscriberId);
        return Map.of("status", "RECOVERED", "subscriberId", subscriberId);
    }

    @GetMapping("/inbox/{subscriberId}")
    public List<Map<String, Object>> inbox(@PathVariable String subscriberId) {
        return inbox.getOrDefault(subscriberId, new CopyOnWriteArrayList<>());
    }

    @GetMapping("/snapshot")
    public BrokerSnapshot snapshot() {
        return broker.snapshot();
    }
}
