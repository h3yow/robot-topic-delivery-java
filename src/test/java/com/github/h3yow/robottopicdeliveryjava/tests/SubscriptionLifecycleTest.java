package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionLifecycleTest {

    private InMemoryMessageBroker broker;

    private InMemoryMessageBroker fastBroker() {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(150);
        properties.setTimeoutScanIntervalMillis(15);
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
    void slowSubscriberRetriesWithoutLosingMessages() throws Exception {
        InMemoryMessageBroker b = fastBroker();
        b.registerTopic("t", String.class, true);
        AtomicInteger seen = new AtomicInteger();
        CountDownLatch allDone = new CountDownLatch(5);
        b.subscribe("slow", "t", (m, c) -> {
            int n = seen.incrementAndGet();
            if (n <= 5) {
                // 前 5 次调用都“处理很慢”：不确认，等超时重试；第 6 次起全部确认。
                return;
            }
            c.ack();
            allDone.countDown();
        }, 2, BackpressurePolicy.BLOCK);

        for (int i = 0; i < 5; i++) {
            b.publish("t", "m" + i);
        }
        assertTrue(allDone.await(10, TimeUnit.SECONDS), "all 5 messages eventually acked");
        BrokerSnapshot snapshot = b.snapshot();
        assertEquals(5, snapshot.stats().getDeliveriesAcked());
        assertEquals(0, snapshot.stats().getDeadDeliveries());
        assertTrue(snapshot.stats().getRetries() >= 5, "retries=" + snapshot.stats().getRetries());
        assertEquals(0, snapshot.subscriptions().get(0).queueSize());
    }

    @Test
    void addingSubscriberDynamicallyOnlyReceivesFutureMessages() throws Exception {
        InMemoryMessageBroker b = fastBroker();
        b.registerTopic("t", String.class, true);
        CountDownLatch earlyDone = new CountDownLatch(2);
        b.subscribe("early", "t", (m, c) -> {
            c.ack();
            earlyDone.countDown();
        });
        b.publish("t", "before-1");
        b.publish("t", "before-2");
        assertTrue(earlyDone.await(3, TimeUnit.SECONDS));

        CountDownLatch lateGot = new CountDownLatch(1);
        List<String> lateMessages = new CopyOnWriteArrayList<>();
        b.subscribe("late", "t", (m, c) -> {
            lateMessages.add(m.payload().toString());
            c.ack();
            lateGot.countDown();
        });
        b.publish("t", "after-1");
        assertTrue(lateGot.await(3, TimeUnit.SECONDS));
        assertEquals(List.of("after-1"), lateMessages);
    }

    @Test
    void removingSubscriberTerminatesPendingWithReasonAndKeepsOthersIntact() throws Exception {
        InMemoryMessageBroker b = fastBroker();
        b.registerTopic("t", String.class, true);
        // removed 订阅者：容量 1，处理器挂住第 1 条，后续 3 条堆积在队列中。
        CountDownLatch firstTaken = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        b.subscribe("removed", "t", (m, c) -> {
            firstTaken.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // 被移除后即便“补确认”也必须无效。
            c.ack();
        }, 3, BackpressurePolicy.BLOCK);

        CountDownLatch otherDone = new CountDownLatch(4);
        b.subscribe("other", "t", (m, c) -> {
            c.ack();
            otherDone.countDown();
        });

        for (int i = 0; i < 4; i++) {
            b.publish("t", "m" + i);
        }
        assertTrue(firstTaken.await(3, TimeUnit.SECONDS));

        b.unsubscribe("removed");
        release.countDown();

        BrokerSnapshot snapshot = b.snapshot();
        // removed 的 4 条（1 在途 + 3 排队）全部确定终止。
        assertEquals(4L,
                snapshot.stats().getDeadByReason().getOrDefault(DeadReason.SUBSCRIBER_REMOVED, 0L));
        // other 订阅者不受影响，全部确认。
        assertTrue(otherDone.await(3, TimeUnit.SECONDS));
        assertEquals(4, b.snapshot().stats().getDeliveriesAcked());
        // 重复移除与不存在的订阅者：安全无异常。
        b.unsubscribe("removed");
        b.unsubscribe("ghost");
    }

    @Test
    void duplicateSubscriberIdAndInvalidFilterAreRejected() {
        InMemoryMessageBroker b = fastBroker();
        Registration r1 = b.subscribe("dup", "t", (m, c) -> c.ack());
        assertEquals(RegistrationStatus.REGISTERED, r1.status());
        Registration r2 = b.subscribe("dup", "t", (m, c) -> c.ack());
        assertEquals(RegistrationStatus.REJECTED, r2.status());
        Registration r3 = b.subscribe("x", "a/#/b", (m, c) -> c.ack());
        assertEquals(RegistrationStatus.REJECTED, r3.status());
    }
}
