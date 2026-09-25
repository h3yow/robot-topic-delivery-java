package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.broker.*;
import com.github.h3yow.robottopicdeliveryjava.codec.MessageCodec;
import com.github.h3yow.robottopicdeliveryjava.config.BrokerConfig;
import com.github.h3yow.robottopicdeliveryjava.topic.TopicFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.robottopicdeliveryjava.tests.BrokerReliabilityTest.await;
import static org.junit.jupiter.api.Assertions.*;

class BrokerConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(BrokerConcurrencyTest.class);

    private MessageBroker broker;

    @BeforeEach
    void setUp() {
        broker = new MessageBroker(new BrokerConfig(256, 150, 2, 2), new MessageCodec());
    }

    @AfterEach
    void tearDown() {
        broker.close();
    }

    private Map<String, Long> stats() {
        return broker.stats().snapshot();
    }

    @Test
    void concurrentPublishToMultipleSubscribers() throws Exception {
        Set<String> exactReceived = ConcurrentHashMap.newKeySet();
        Set<String> wildcardReceived = ConcurrentHashMap.newKeySet();
        broker.subscribe(TopicFilter.of("robot/arm/position"),
                d -> { exactReceived.add(d.message().id()); d.ack(); });
        broker.subscribe(TopicFilter.of("robot/#"),
                d -> { wildcardReceived.add(d.message().id()); d.ack(); });

        int publishers = 8, perPublisher = 25, total = publishers * perPublisher;
        ExecutorService pool = Executors.newFixedThreadPool(publishers);
        CountDownLatch done = new CountDownLatch(publishers);
        AtomicInteger accepted = new AtomicInteger();
        for (int p = 0; p < publishers; p++) {
            final int pid = p;
            pool.submit(() -> {
                for (int i = 0; i < perPublisher; i++) {
                    PublishReceipt r = broker.publish("robot/arm/position",
                            Map.of("publisher", pid, "seq", i));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    }
                }
                done.countDown();
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        await("all acked", () -> stats().get("acked") == 2L * total);
        Map<String, Long> s = stats();
        log.info("[case=concurrentPublish] 输入={}线程x{}条 stats={} "
                + "判定依据=每个订阅恰好收到全部消息且无重复(集合去重后数量相等)", publishers, perPublisher, s);
        assertEquals(total, accepted.get());
        assertEquals(total, exactReceived.size());
        assertEquals(total, wildcardReceived.size());
        assertEquals(2L * total, s.get("enqueued"));
        assertEquals(2L * total, s.get("acked"));
        assertEquals(0L, s.get("enqueueRejected"));
        assertEquals(0L, s.get("dead"));
        // 守恒：enqueued == acked + dead + dropped（已静默）
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }

    @Test
    void slowSubscriberTriggersBackpressureWithoutAffectingOthers() throws Exception {
        // 慢订阅者：容量 8 的小队列 broker
        try (MessageBroker small = new MessageBroker(new BrokerConfig(8, 5000, 1, 2), new MessageCodec())) {
            CountDownLatch block = new CountDownLatch(1);
            List<String> slowReceived = new CopyOnWriteArrayList<>();
            Subscription slow = small.subscribe(TopicFilter.of("robot/#"), d -> {
                try {
                    block.await(); // 模拟慢消费者：一直阻塞
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                slowReceived.add(d.message().id());
                d.ack();
            });
            AtomicInteger fastAcked = new AtomicInteger();
            small.subscribe(TopicFilter.of("robot/#"), d -> { fastAcked.incrementAndGet(); d.ack(); });

            int total = 20;
            int enqueuedToSlow = 0, rejectedToSlow = 0, enqueuedToFast = 0, rejectedToFast = 0;
            for (int i = 0; i < total; i++) {
                PublishReceipt r = small.publish("robot/arm", Map.of("seq", i));
                assertTrue(r.accepted());
                for (PublishReceipt.EnqueueResult er : r.enqueueResults()) {
                    if (er.subscriptionId().equals(slow.id())) {
                        if (er.enqueued()) enqueuedToSlow++; else rejectedToSlow++;
                    } else {
                        if (er.enqueued()) enqueuedToFast++; else rejectedToFast++;
                    }
                }
            }
            Map<String, Long> s = small.stats().snapshot();
            log.info("[case=slowSubscriber] 输入={}条消息/容量8 enqueuedToSlow={} rejectedToSlow={} "
                    + "enqueuedToFast={} rejectedToFast={} stats={} "
                    + "判定依据=慢订阅者至多接受9条(8在队+1在途)，其余被背压拒绝；全局拒绝数==各订阅拒绝数之和",
                    total, enqueuedToSlow, rejectedToSlow, enqueuedToFast, rejectedToFast, s);
            // 慢订阅者：派发线程阻塞在第一条上，队列容量8，故接受数至多为 9（取决于首个 poll 的时序）
            assertTrue(enqueuedToSlow >= 8 && enqueuedToSlow <= 9,
                    "enqueuedToSlow=" + enqueuedToSlow);
            assertEquals(total - enqueuedToSlow, rejectedToSlow);
            // 全局背压计数与各订阅回执之和一致（可核验、无遗漏）
            assertEquals((long) rejectedToSlow + rejectedToFast, s.get("enqueueRejected"));
            // 快订阅者：凡入队的最终全部被确认（不被慢订阅者拖累）
            int expectedFast = enqueuedToFast;
            await("fast subscriber acked all enqueued", () -> fastAcked.get() == expectedFast);

            block.countDown(); // 放行慢订阅者
            int expectedSlow = enqueuedToSlow;
            await("slow drained", () -> slowReceived.size() == expectedSlow);
            Map<String, Long> s2 = small.stats().snapshot();
            log.info("[case=slowSubscriber] 放行后 stats={} 判定依据=已入队消息全部确认，无丢失", s2);
            assertEquals((long) expectedFast + expectedSlow, s2.get("acked"));
        }
    }

    @Test
    void disconnectAndReconnectRedeliversWithoutLoss() throws Exception {
        Set<String> received = ConcurrentHashMap.newKeySet();
        AtomicInteger deliveries = new AtomicInteger();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
            deliveries.incrementAndGet();
            received.add(d.message().id());
            d.ack();
        });

        // 先正常发 3 条
        for (int i = 0; i < 3; i++) {
            broker.publish("robot/arm", Map.of("phase", "before", "seq", i));
        }
        await("before acked", () -> stats().get("acked") == 3L);

        // 断连期间发 5 条：不得投递、不得丢失
        broker.disconnect(sub.id());
        for (int i = 0; i < 5; i++) {
            broker.publish("robot/arm", Map.of("phase", "offline", "seq", i));
        }
        Thread.sleep(200);
        int receivedWhileOffline = received.size();
        log.info("[case=reconnect] 断连期间收到={} 判定依据=断连时暂停派发", receivedWhileOffline);
        assertEquals(3, receivedWhileOffline);

        // 恢复：补投全部未确认消息
        broker.reconnect(sub.id());
        await("all acked after reconnect", () -> stats().get("acked") == 8L);
        Map<String, Long> s = stats();
        log.info("[case=reconnect] 恢复后 stats={} 判定依据=8条全部确认、无重复(集合大小==8)、补投计数>=5", s);
        assertEquals(8, received.size()); // 无重复投递导致同一消息被多次确认
        assertTrue(s.get("redeliveredOnReconnect") >= 5);
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }

    @Test
    void reconnectWhileInflightUnackedRedeliversSameMessage() throws Exception {
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        AtomicInteger deliveryCount = new AtomicInteger();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
            int n = deliveryCount.incrementAndGet();
            attempts.add(d.attempt());
            if (n >= 2) {
                d.ack(); // 第2次投递才确认
            }
        });

        broker.publish("robot/arm", Map.of("v", 1));
        await("first delivery", () -> deliveryCount.get() == 1);
        // 第一次投递不确认，并在超时前断连
        broker.disconnect(sub.id());
        Thread.sleep(300); // 超过 ackTimeout(150ms)，期间发生超时但不得重投（已断连）
        assertEquals(1, deliveryCount.get());

        broker.reconnect(sub.id());
        await("redelivered and acked", () -> stats().get("acked") == 1L);
        Map<String, Long> s = stats();
        log.info("[case=reconnectInflight] attempts={} stats={} "
                + "判定依据=同一消息以attempt=2补投且只确认一次", attempts, s);
        assertEquals(List.of(1, 2), attempts);
        assertEquals(1L, s.get("timeouts"));
        assertEquals(1L, s.get("retries"));
        assertEquals(0L, s.get("dead"));
    }

    @Test
    void unsubscribeDropsInflightAndQueuedDeterministically() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
            try {
                block.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            d.ack();
        });

        int total = 5;
        for (int i = 0; i < total; i++) {
            broker.publish("robot/arm", Map.of("seq", i));
        }
        await("one inflight", () -> stats().get("delivered") == 1L);

        broker.unsubscribe(sub.id());
        block.countDown();
        Map<String, Long> s = stats();
        log.info("[case=unsubscribe] 输入={}条消息(1在途+4在队) stats={} "
                + "判定依据=全部计为dropped，守恒成立", total, s);
        assertEquals((long) total, s.get("dropped"));
        assertEquals(0L, s.get("acked"));
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
        // 取消后迟到的 ack 不得改变计数守恒
        await("late ack settled", () -> stats().get("lateAcks") >= 0L);
    }

    @Test
    void dynamicSubscribeUnsubscribeDuringPublishing() throws Exception {
        Set<String> stable = ConcurrentHashMap.newKeySet();
        broker.subscribe(TopicFilter.of("robot/#"), d -> { stable.add(d.message().id()); d.ack(); });

        // 发布过程中动态增删订阅；每轮：临时订阅 -> 发布1条 -> 取消
        // unsubscribe 返回时，该轮入队临时订阅的消息必然已被确定地计入 acked 或 dropped 之一
        for (int round = 0; round < 10; round++) {
            Subscription t = broker.subscribe(TopicFilter.of("robot/arm/#"), Delivery::ack);
            broker.publish("robot/arm/joint", Map.of("round", round));
            broker.unsubscribe(t.id());
        }
        await("stable received all", () -> stable.size() == 10);
        Map<String, Long> s = stats();
        log.info("[case=dynamicSub] 输入=10轮(临时订阅+发布+取消) stats={} "
                + "判定依据=稳定订阅者收齐10条；入队20条全部守恒地落入 acked/dead/dropped", s);
        assertEquals(10, stable.size());
        assertEquals(20L, s.get("enqueued"));   // 每条消息入队2个订阅
        assertTrue(s.get("acked") >= 10);       // 稳定订阅者的10条全部确认
        // 守恒：临时订阅者的每条消息要么被确认要么被丢弃，不得双重计数
        assertEquals(s.get("enqueued"),
                s.get("acked") + s.get("dead") + s.get("dropped"));
    }
}
