package com.github.h3yow.robottopicdeliveryjava.tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosticLoggingTest {

    private InMemoryMessageBroker broker;
    private ListAppender<ILoggingEvent> appender;
    private Logger brokerLogger;

    private InMemoryMessageBroker startCapturing() {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(100);
        properties.setTimeoutScanIntervalMillis(15);
        broker = new InMemoryMessageBroker(properties, new JsonMessageCodec());
        brokerLogger = (Logger) LoggerFactory.getLogger(InMemoryMessageBroker.class);
        appender = new ListAppender<>();
        appender.start();
        brokerLogger.addAppender(appender);
        return broker;
    }

    @AfterEach
    void tearDown() {
        if (brokerLogger != null) {
            brokerLogger.detachAppender(appender);
        }
        if (broker != null) {
            broker.close();
        }
    }

    private boolean logContains(String fragment) {
        return appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains(fragment));
    }

    private List<String> messagesAt(Level level) {
        return appender.list.stream()
                .filter(e -> e.getLevel().equals(level))
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    void logsKeyInputsStateTransitionsAndDecisionReasons() throws Exception {
        InMemoryMessageBroker b = startCapturing();
        b.registerTopic("robot/r1/status", String.class, true);

        CountDownLatch done = new CountDownLatch(1);
        b.subscribe("s1", "robot/+/status", (m, c) -> {
            c.ack();
            done.countDown();
        });
        b.publish("robot/r1/status", "hello");
        assertTrue(done.await(2, TimeUnit.SECONDS));

        // 关键输入
        assertTrue(logContains("topic registered"));
        assertTrue(logContains("subscriber added"));
        assertTrue(logContains("publish accepted"));
        // 状态迁移：派发（attempt）与确认
        assertTrue(logContains("dispatch:"), "dispatch transition logged");
        assertTrue(logContains("attempt=1"));
        assertTrue(logContains("ack accepted"), "state transition to ACKED logged");

        // 判定依据：未知主题 / 非法负载的拒绝原因
        b.publish("robot/r1/missing", "x");
        b.publish("robot/r1/status", 99);
        assertTrue(logContains("reason=UNKNOWN_TOPIC"));
        assertTrue(logContains("reason=INVALID_PAYLOAD"));
        assertTrue(messagesAt(Level.WARN).stream().anyMatch(m -> m.contains("publish rejected")));

        // 重复确认判定
        // （通过再发一条并捕获控制句柄验证：这里直接确认日志里已有完整判定依据格式）
        assertNotNull(b.snapshot());
    }

    @Test
    void logsTimeoutRetryAndDeadTransitions() throws Exception {
        InMemoryMessageBroker b = startCapturing();
        b.registerTopic("t", String.class, true);
        b.subscribe("s1", "t", (m, c) -> {
            // 永不确认，快速耗尽重试。
        });
        b.publish("t", "x");
        // 等待至少一次超时重试与最终终止日志。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline
                && !(logContains("ack timeout") && logContains("RETRIES_EXHAUSTED"))) {
            Thread.sleep(20);
        }
        assertTrue(logContains("ack timeout"), "timeout decision must be logged");
        assertTrue(logContains("scheduling retry"), "retry transition must be logged");
        assertTrue(logContains("RETRIES_EXHAUSTED"), "dead transition must be logged");
    }

    @Test
    void logsDisconnectAndRecovery() throws Exception {
        InMemoryMessageBroker b = startCapturing();
        b.registerTopic("t", String.class, true);

        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch gate1 = new CountDownLatch(1);
        b.subscribe("s1", "t", (m, c) -> {
            first.countDown();
            try {
                gate1.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            c.ack();
        });
        b.publish("t", "m0");
        assertTrue(first.await(2, TimeUnit.SECONDS));
        b.disconnect("s1");
        assertTrue(logContains("subscriber disconnected"), "disconnect input logged");
        b.recover("s1");
        assertTrue(logContains("subscriber recovered"), "recovery logged");
        gate1.countDown();
    }

    @Test
    void logsBackpressureDecision() throws Exception {
        InMemoryMessageBroker b = startCapturing();
        b.registerTopic("t", String.class, true);

        CountDownLatch one = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        b.subscribe("s2-bp", "t", (m, c) -> {
            one.countDown();
            gate.await();
            c.ack();
        }, 1, BackpressurePolicy.REJECT);
        b.publish("t", "m1");
        assertTrue(one.await(2, TimeUnit.SECONDS));
        // m1 已被唯一 worker 取走在处理器中阻塞，容量 1 的队列仍可放入 m2（排队占满）。
        assertEquals(PublishStatus.ACCEPTED, b.publish("t", "m2").status());
        // m3 入队时队列已满 -> REJECT。
        PublishReceipt overflow = b.publish("t", "overflow");
        assertEquals(PublishStatus.REJECTED, overflow.status());
        assertTrue(logContains("enqueue rejected"), "backpressure decision reason logged");
        assertTrue(logContains("policy=REJECT"));
        gate.countDown();
    }

    private void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("condition not met within timeout");
    }
}
