package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionRecoveryTest {

    private InMemoryMessageBroker broker;

    private BrokerProperties props(long graceMillis) {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(60_000);
        properties.setTimeoutScanIntervalMillis(20);
        properties.setDisconnectGraceMillis(graceMillis);
        broker = new InMemoryMessageBroker(properties, new JsonMessageCodec());
        return properties;
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void inflightMessagesAreRedeliveredAfterRecoveryWithoutDuplicateAck() throws Exception {
        InMemoryMessageBroker b = new InMemoryMessageBroker(props(60_000), new JsonMessageCodec());
        b.registerTopic("t", String.class, true);

        // 第 1 轮收到但不确认；断开 -> 恢复 -> 第 2 轮确认。记录每次投递的 attempt。
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        CountDownLatch firstDelivery = new CountDownLatch(1);
        CountDownLatch acked = new CountDownLatch(1);
        b.subscribe("s1", "t", (m, c) -> {
            attempts.add(m.attempt());
            if (m.messageId().equals("msg-1") && m.attempt() == 1) {
                firstDelivery.countDown();
                return; // 模拟 m1 在途但尚未确认时连接中断。
            }
            c.ack();
            acked.countDown();
        });

        b.publish("t", "m1");
        assertTrue(firstDelivery.await(3, TimeUnit.SECONDS));
        b.disconnect("s1");

        // 断开期间新消息进入队列等待，不调用处理器。
        b.publish("t", "m2");
        Thread.sleep(150);
        assertEquals(List.of(1), List.copyOf(attempts), "handler must not be invoked while disconnected");

        b.recover("s1");
        assertTrue(acked.await(3, TimeUnit.SECONDS), "in-flight m1 redelivered and acked");
        // m1 补投确认后，断开期间排队的 m2 也要送达并确认（同一个 handler 会确认它）。
        waitUntil(() -> b.snapshot().stats().getDeliveriesAcked() == 2, 3_000);
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(2, snapshot.stats().getDeliveriesAcked());
        assertEquals(0, snapshot.stats().getDeadDeliveries());
        assertEquals(0, snapshot.stats().getDuplicateAcks());
    }

    @Test
    void disconnectedSubscriberPastGracePeriodDeadLettersWithConnectionLost() throws Exception {
        InMemoryMessageBroker b = new InMemoryMessageBroker(props(200), new JsonMessageCodec());
        b.registerTopic("t", String.class, true);
        CountDownLatch first = new CountDownLatch(1);
        b.subscribe("s1", "t", (m, c) -> {
            first.countDown(); // 不确认
        }, 4, BackpressurePolicy.BLOCK);

        b.publish("t", "m1");
        b.publish("t", "m2"); // 队列中等待
        assertTrue(first.await(3, TimeUnit.SECONDS));
        b.disconnect("s1");

        waitUntil(() -> b.snapshot().stats().getDeadByReason()
                .getOrDefault(DeadReason.CONNECTION_LOST, 0L) >= 2, 5_000);
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(2L, snapshot.stats().getDeadByReason().get(DeadReason.CONNECTION_LOST));
        // 宽限期后恢复不复活已终止消息。
        b.recover("s1");
        Thread.sleep(200);
        assertEquals(0, b.snapshot().stats().getDeliveriesAcked());
    }

    @Test
    void disconnectAndRecoverUnknownSubscriberAreSafe() {
        InMemoryMessageBroker b = new InMemoryMessageBroker(props(1_000), new JsonMessageCodec());
        b.disconnect("ghost");
        b.recover("ghost");
        assertEquals(0, b.snapshot().stats().getDeadDeliveries());
    }

    private void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("condition not met within timeout");
    }
}
