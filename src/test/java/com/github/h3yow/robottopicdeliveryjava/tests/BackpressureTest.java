package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BackpressureTest {

    private InMemoryMessageBroker broker;

    private BrokerProperties props() {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(60_000); // 测试中不希望超时干扰队列水位
        return properties;
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void rejectPolicyReturnsDistinguishableReceiptAndCountsRejection() throws Exception {
        broker = new InMemoryMessageBroker(props(), new JsonMessageCodec());
        broker.registerTopic("t", String.class, true);
        CountDownLatch gate = new CountDownLatch(1);
        // 容量 2：第一条被处理器取走但挂住，队列还能再放 2 条。
        broker.subscribe("slow", "t", (m, c) -> gate.await(), 2, BackpressurePolicy.REJECT);

        Thread.sleep(100); // 确保 worker 已在 take() 等待
        assertEquals(PublishStatus.ACCEPTED, broker.publish("t", "m1").status());
        Thread.sleep(50);  // m1 被 worker 取走（在处理器中挂住）
        assertEquals(PublishStatus.ACCEPTED, broker.publish("t", "m2").status());
        assertEquals(PublishStatus.ACCEPTED, broker.publish("t", "m3").status());
        PublishReceipt rejected = broker.publish("t", "m4");
        assertEquals(PublishStatus.REJECTED, rejected.status());
        assertTrue(rejected.reason().contains("REJECT policy"), rejected.reason());

        BrokerSnapshot snapshot = broker.snapshot();
        assertTrue(snapshot.stats().getRejectedEnqueues() >= 1);
        assertTrue(snapshot.stats().getDeadByReason()
                .containsKey(DeadReason.QUEUE_FULL_REJECTED));
        assertEquals(2, snapshot.subscriptions().get(0).queueSize());
        gate.countDown();
    }

    @Test
    void dropOldestPolicyEvictsDeterministicallyAndDeadLetterCounts() throws Exception {
        broker = new InMemoryMessageBroker(props(), new JsonMessageCodec());
        broker.registerTopic("t", String.class, true);
        CountDownLatch gate = new CountDownLatch(1);
        broker.subscribe("slow", "t", (m, c) -> gate.await(), 2, BackpressurePolicy.DROP_OLDEST);

        Thread.sleep(100);
        broker.publish("t", "m1");
        Thread.sleep(50);
        broker.publish("t", "m2"); // 入队
        broker.publish("t", "m3"); // 入队，满
        PublishReceipt r4 = broker.publish("t", "m4"); // 淘汰 m2，m4 入队
        PublishReceipt r5 = broker.publish("t", "m5"); // 淘汰 m3，m5 入队
        assertEquals(PublishStatus.ACCEPTED, r4.status());
        assertEquals(PublishStatus.ACCEPTED, r5.status());

        BrokerSnapshot snapshot = broker.snapshot();
        // m1 在途、m2/m3 被淘汰为 DEAD(DROPPED_OLDEST)、m4/m5 在队列。
        Long dropped = snapshot.stats().getDeadByReason().get(DeadReason.DROPPED_OLDEST);
        assertEquals(2L, dropped);
        assertEquals(2, snapshot.subscriptions().get(0).queueSize());
        gate.countDown();
    }

    @Test
    void blockPolicyAppliesBackpressureToPublisherWithoutLoss() throws Exception {
        broker = new InMemoryMessageBroker(props(), new JsonMessageCodec());
        broker.registerTopic("t", String.class, true);
        CountDownLatch processed = new CountDownLatch(4);
        broker.subscribe("s1", "t", (m, c) -> {
            c.ack();
            processed.countDown();
        }, 1, BackpressurePolicy.BLOCK);

        Thread publisher = new Thread(() -> {
            for (int i = 0; i < 4; i++) {
                broker.publish("t", "m" + i);
            }
        }, "test-publisher");
        publisher.start();
        assertTrue(processed.await(5, TimeUnit.SECONDS));
        publisher.join(2_000);
        assertFalse(publisher.isAlive(), "publisher should have drained under BLOCK policy");
        BrokerSnapshot snapshot = broker.snapshot();
        assertEquals(4, snapshot.stats().getPublishedAccepted());
        assertEquals(4, snapshot.stats().getDeliveriesAcked());
        assertEquals(0, snapshot.stats().getRejectedEnqueues());
        assertEquals(0, snapshot.stats().getDeadDeliveries());
    }
}
