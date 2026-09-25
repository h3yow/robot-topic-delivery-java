package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentPublishTest {

    private InMemoryMessageBroker broker;

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void concurrentPublishingReachesEverySubscriberExactlyOnceWithConsistentCounters() throws Exception {
        BrokerProperties properties = new BrokerProperties();
        properties.setAckTimeoutMillis(10_000);
        properties.setQueueCapacity(512);
        broker = new InMemoryMessageBroker(properties, new JsonMessageCodec());
        broker.registerTopic("robot/area-a/status", String.class, false);
        broker.registerTopic("robot/area-b/status", String.class, false);

        int subscribers = 3;
        int publishers = 8;
        int perPublisher = 250;
        int total = publishers * perPublisher;          // 2000 条
        int perTopic = total / 2;                       // 交错发布，每主题恰好 1000

        // s1/s2 订阅全部主题（各收 total=2000），s3 只订阅 area-a（收 perTopic=1000）。
        ConcurrentHashMap<String, AtomicInteger>[] received = new ConcurrentHashMap[subscribers];
        CountDownLatch[] latches = new CountDownLatch[subscribers];
        long[] expected = {total, total, perTopic};
        for (int i = 0; i < subscribers; i++) {
            received[i] = new ConcurrentHashMap<>();
            latches[i] = new CountDownLatch((int) expected[i]);
            final int idx = i;
            String filter = i == 2 ? "robot/area-a/#" : "robot/#";
            broker.subscribe("s" + i, filter, (m, c) -> {
                // 以 messageId 去重统计：若发生错误重复投递，这里会出现重复键计数 > 1。
                received[idx].computeIfAbsent(m.messageId(), k -> new AtomicInteger()).incrementAndGet();
                c.ack();
                latches[idx].countDown();
            }, 1024, BackpressurePolicy.BLOCK);
        }

        ExecutorService pool = Executors.newFixedThreadPool(publishers);
        for (int p = 0; p < publishers; p++) {
            final int publisherId = p;
            pool.submit(() -> {
                for (int i = 0; i < perPublisher; i++) {
                    String topic = (i + publisherId) % 2 == 0
                            ? "robot/area-a/status" : "robot/area-b/status";
                    PublishReceipt receipt = broker.publish(topic, "p" + publisherId + "-" + i);
                    if (receipt.status() != PublishStatus.ACCEPTED) {
                        throw new AssertionError("unexpected rejection: " + receipt.reason());
                    }
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "publishers should finish");

        for (int i = 0; i < subscribers; i++) {
            boolean ok = latches[i].await(30, TimeUnit.SECONDS);
            assertTrue(ok, "subscriber s" + i + " missed messages, remaining=" + latches[i].getCount());
            // 每条消息恰好被收到一次：无丢失、无错误重复。
            long dupCount = received[i].values().stream().filter(v -> v.get() > 1).count();
            assertEquals(0, dupCount, "subscriber s" + i + " saw duplicate deliveries");
            assertEquals(expected[i], received[i].size());
        }

        BrokerSnapshot snapshot = broker.snapshot();
        assertEquals(total, snapshot.stats().getPublishedAccepted());
        assertEquals(0, snapshot.stats().getPublishedRejected());
        // s0+s1 各 2000、s2 1000 = 5000 次投递，全部 ACKED。
        assertEquals(5_000L, snapshot.stats().getDeliveriesAcked());
        assertEquals(5_000L, snapshot.stats().getDeliveriesEnqueued());
        assertEquals(0, snapshot.stats().getDeadDeliveries());
        assertEquals(0, snapshot.stats().getDuplicateAcks());
        assertEquals(0, snapshot.stats().getLateAcks());
        for (SubscriptionSnapshot s : snapshot.subscriptions()) {
            assertEquals(0, s.queueSize(), "queue drained for " + s.subscriberId());
        }
    }
}
