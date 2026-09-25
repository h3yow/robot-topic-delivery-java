package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class QueueSaturationTest {

    private InMemoryMessageBroker broker;

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void saturationIsObservableAndDrainsAfterSubscriberCatchesUp() throws Exception {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(60_000);
        broker = new InMemoryMessageBroker(properties, new JsonMessageCodec());
        broker.registerTopic("t", String.class, true);

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch allDone = new CountDownLatch(5);
        CountDownLatch oneInFlight = new CountDownLatch(1);
        broker.subscribe("slow", "t", (m, c) -> {
            // 第一条进入后立即报告并阻塞；worker 是单线程，后续消息必然滞留队列。
            if ("m0".equals(m.payload())) {
                oneInFlight.countDown();
                gate.await();
            }
            c.ack();
            allDone.countDown();
        }, 4, BackpressurePolicy.REJECT);

        broker.publish("t", "m0");
        assertTrue(oneInFlight.await(3, TimeUnit.SECONDS));
        int accepted = 0;
        int rejected = 0;
        for (int i = 1; i < 10; i++) {
            PublishReceipt receipt = broker.publish("t", "m" + i);
            if (receipt.status() == PublishStatus.ACCEPTED) {
                accepted++;
            } else {
                rejected++;
                assertTrue(receipt.reason().contains("QUEUE") || receipt.reason().contains("REJECT"),
                        receipt.reason());
            }
        }
        // m0 在处理器中；队列容量 4，m1..m4 排队（4 条），m5..m9 拒绝（5 条）。
        assertEquals(4, accepted);
        assertEquals(5, rejected);
        BrokerSnapshot saturated = broker.snapshot();
        assertEquals(4, saturated.subscriptions().get(0).queueSize());
        assertEquals(5, saturated.stats().getRejectedEnqueues());
        assertEquals(5L, saturated.stats().getDeadByReason().get(DeadReason.QUEUE_FULL_REJECTED));

        gate.countDown(); // 慢订阅者恢复处理能力，m0 + m1..m4 共 5 条全部确认。
        assertTrue(allDone.await(5, TimeUnit.SECONDS));
        waitUntil(() -> broker.snapshot().subscriptions().get(0).queueSize() == 0, 3_000);
        BrokerSnapshot drained = broker.snapshot();
        assertEquals(5, drained.stats().getDeliveriesAcked());
        assertEquals(5, drained.stats().getDeadDeliveries());
        // 守恒：接受 5（=acked 5），拒绝 5（=dead 5）。
        assertEquals(5 + 5, drained.stats().getDeliveriesAcked() + drained.stats().getDeadDeliveries());
    }

    private void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(15);
        }
        fail("condition not met within timeout");
    }
}
