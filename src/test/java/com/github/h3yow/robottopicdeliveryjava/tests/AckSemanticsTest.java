package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AckSemanticsTest {

    private InMemoryMessageBroker broker;

    private InMemoryMessageBroker fastBroker(int maxAttempts) {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(200);
        properties.setTimeoutScanIntervalMillis(20);
        properties.setMaxAttempts(maxAttempts);
        broker = new InMemoryMessageBroker(properties, new JsonMessageCodec());
        return broker;
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void duplicateAckIsIdempotentAndCounted() throws Exception {
        InMemoryMessageBroker b = fastBroker(3);
        b.registerTopic("t", String.class, true);
        CountDownLatch firstDispatch = new CountDownLatch(1);
        AtomicReference<MessageHandler.DeliveryControl> firstControl = new AtomicReference<>();
        b.subscribe("s1", "t", (m, c) -> {
            if (firstControl.compareAndSet(null, c)) {
                c.ack();
                firstDispatch.countDown();
            } else {
                fail("must not redeliver after ack");
            }
        });

        b.publish("t", "x");
        assertTrue(firstDispatch.await(2, TimeUnit.SECONDS));
        // 对同一条已确认消息再次确认：必须返回 false 且计为重复确认。
        boolean second = firstControl.get().ack();
        boolean third = firstControl.get().ack();
        assertFalse(second);
        assertFalse(third);

        waitUntil(() -> b.snapshot().stats().getDuplicateAcks() == 2, 2_000);
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(1, snapshot.stats().getDeliveriesAcked());
        assertEquals(2, snapshot.stats().getDuplicateAcks());
        assertEquals(0, snapshot.stats().getRetries());
    }

    @Test
    void noAckCausesTimeoutRetryThenAckOnSecondAttempt() throws Exception {
        InMemoryMessageBroker b = fastBroker(3);
        b.registerTopic("t", String.class, true);
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        CountDownLatch ackedAttempt2 = new CountDownLatch(1);
        b.subscribe("s1", "t", (m, c) -> {
            attempts.add(m.attempt());
            if (m.attempt() == 2) {
                c.ack();
                ackedAttempt2.countDown();
            }
            // attempt=1 故意不确认，等待超时扫描重投。
        });

        b.publish("t", "x");
        assertTrue(ackedAttempt2.await(3, TimeUnit.SECONDS));
        waitUntil(() -> attempts.size() == 2, 2_000);
        assertEquals(List.of(1, 2), List.copyOf(attempts));
        waitUntil(() -> b.snapshot().stats().getDeliveriesAcked() == 1, 2_000);
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(1, snapshot.stats().getDeliveriesAcked());
        assertTrue(snapshot.stats().getRetries() >= 1, "retries=" + snapshot.stats().getRetries());
        assertEquals(0, snapshot.stats().getDeadDeliveries());
    }

    @Test
    void lateAckAfterTimeoutIsRejectedAndCountedWithoutRegression() throws Exception {
        InMemoryMessageBroker b = fastBroker(3);
        b.registerTopic("t", String.class, true);
        List<MessageHandler.DeliveryControl> controls = new CopyOnWriteArrayList<>();
        CountDownLatch attempt2Acked = new CountDownLatch(1);
        b.subscribe("s1", "t", (m, c) -> {
            controls.add(c);
            if (m.attempt() >= 2) {
                c.ack();
                attempt2Acked.countDown();
            }
        });

        b.publish("t", "x");
        assertTrue(attempt2Acked.await(3, TimeUnit.SECONDS));
        // 第 1 轮的确认句柄迟到：记录已在第 2 轮被确认，ack() 返回 false（幂等拒绝），
        // 状态保持 ACKED，且不会产生任何额外投递。
        boolean late = controls.get(0).ack();
        assertFalse(late);
        waitUntil(() -> b.snapshot().stats().getDuplicateAcks() >= 1, 2_000);
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(1, snapshot.stats().getDeliveriesAcked());
        // 初次入队 1 次（不随重试增加），重计入 retries；实际派发 2 次（attempts 序列）。
        assertEquals(1, snapshot.stats().getDeliveriesEnqueued());
        assertTrue(snapshot.stats().getRetries() >= 1);
    }

    @Test
    void exhaustedRetriesReachDeadTerminalAndStopRedelivery() throws Exception {
        InMemoryMessageBroker b = fastBroker(2);
        b.registerTopic("t", String.class, true);
        CountDownLatch attemptsSeen = new CountDownLatch(2);
        CopyOnWriteArrayList<MessageHandler.DeliveryControl> controls = new CopyOnWriteArrayList<>();
        b.subscribe("s1", "t", (m, c) -> {
            controls.add(c);
            attemptsSeen.countDown();
            // 从不确认。
        });

        b.publish("t", "x");
        assertTrue(attemptsSeen.await(3, TimeUnit.SECONDS));
        waitUntil(() -> b.snapshot().stats().getDeadByReason()
                .getOrDefault(DeadReason.RETRIES_EXHAUSTED, 0L) == 1, 3_000);
        Thread.sleep(500); // 再等一个扫描周期，确认不会继续投递。
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(1, snapshot.stats().getDeadDeliveries());
        assertEquals(0, snapshot.stats().getDeliveriesAcked());
        assertEquals(1, snapshot.stats().getDeadByReason().get(DeadReason.RETRIES_EXHAUSTED));
        // 处理器恰好被调用 maxAttempts 次。
        assertEquals(2, controls.size());
        // 终止后的确认属于迟到确认：返回 false，计 lateAcks，状态不倒退。
        assertFalse(controls.get(0).ack());
        waitUntil(() -> b.snapshot().stats().getLateAcks() >= 1, 2_000);
        assertEquals(1, b.snapshot().stats().getDeadDeliveries());
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
