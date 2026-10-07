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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重试退避能力测试：
 * - 重试之间按 initial * multiplier^(n-1) 逐次变长等待（下界严格断言，可复现）；
 * - 等待用线程等待而非自旋，不空转 CPU；
 * - 一个订阅退避等待不连累其他订阅；
 * - 等待期间断连：不断连结果确定、不丢不重，恢复后只补投一次；
 * - 等待期间移除订阅：消息确定性 dropped，不进留存、不重投；
 * - 退避产生的状态/数字进入统计与诊断。
 */
class RetryBackoffTest {

    private static final Logger log = LoggerFactory.getLogger(RetryBackoffTest.class);

    // 确认超时 60ms；退避 100ms 起步、2 倍递增、上限 2000ms；最多重试 3 次
    private static final long ACK_TIMEOUT = 60;
    private static final long B0 = 100, MULT = 2;

    private MessageBroker broker;

    @BeforeEach
    void setUp() {
        broker = new MessageBroker(
                new BrokerConfig(16, ACK_TIMEOUT, 3, 2, B0, MULT, 2000, 256, DeadLetterOverflowPolicy.EVICT_OLDEST),
                new MessageCodec());
    }

    @AfterEach
    void tearDown() {
        broker.close();
    }

    static void await(String what, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for: " + what);
            }
            Thread.sleep(2);
        }
    }

    private Map<String, Long> stats() {
        return broker.stats().snapshot();
    }

    @Test
    void retriesWaitWithIncreasingBackoffLowerBound() throws Exception {
        CopyOnWriteArrayList<Long> times = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/#"), d -> times.add(System.nanoTime())); // 永不确认

        long start = System.nanoTime();
        broker.publish("robot/arm", Map.of("v", 1));
        await("4 delivery attempts (1 + 3 retries)", () -> times.size() == 4);

        long t0Ms = (times.get(0) - start) / 1_000_000;
        long gap1 = (times.get(1) - times.get(0)) / 1_000_000;
        long gap2 = (times.get(2) - times.get(1)) / 1_000_000;
        long gap3 = (times.get(3) - times.get(2)) / 1_000_000;

        // 确认超时等待按 50ms 节拍唤醒，超时点最多被提前一个节拍检测；时间戳取于监听器入口，
        // 故下界扣除该节拍与少量调度抖动，仍严格保留 "ackTimeout + 退避" 的可辨认间隔。
        long tickTolerance = 55;
        log.info("[case=backoffTiming] attemptGapsMs=[{},{},{}] (ackTimeout={}, backoff=100,200,400) "
                + "判定依据=每次间隔 >= ackTimeout+对应退避-节拍容差，且逐次严格变长", gap1, gap2, gap3, ACK_TIMEOUT);
        assertTrue(t0Ms < ACK_TIMEOUT + 200, "首次投递应几乎立即发生，实际=" + t0Ms);
        assertTrue(gap1 >= ACK_TIMEOUT + B0 - tickTolerance, "第1次重试至少等待 timeout+100ms，实际=" + gap1);
        assertTrue(gap2 >= ACK_TIMEOUT + B0 * MULT - tickTolerance, "第2次重试至少等待 timeout+200ms，实际=" + gap2);
        assertTrue(gap3 >= ACK_TIMEOUT + B0 * MULT * MULT - tickTolerance, "第3次重试至少等待 timeout+400ms，实际=" + gap3);
        assertTrue(gap2 > gap1, "退避逐次变长");
        assertTrue(gap3 > gap2, "退避逐次变长");

        Map<String, Long> s = stats();
        assertEquals(3L, s.get("backoffWaits"));
        assertEquals(3L, s.get("retries"));
        long expectedBackoff = B0 + (long) (B0 * MULT) + (long) (B0 * MULT * MULT); // 100+200+400
        assertEquals(expectedBackoff, s.get("backoffMillis"), "累计计划退避时长精确记账");
        await("dead after retries exhausted", () -> stats().get("dead") == 1L);
        assertEquals(4L, stats().get("delivered"), "共投递4次");
    }

    @Test
    void backoffWaitsDoNotSpinCpu() throws Exception {
        // 永不确认：制造一次长退避（初始100ms）。等待期间派发线程应处于 WAITING/TIMED_WAITING 而非 RUNNABLE。
        CopyOnWriteArrayList<Thread> listenerThreads = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/#"), d -> {
            listenerThreads.add(Thread.currentThread());
        });
        broker.publish("robot/arm", Map.of("v", 1));
        await("first attempt done and backing off", () -> stats().get("backoffWaits") >= 1L);

        // 找到该订阅的派发线程，在退避窗口内多次采样其状态
        Thread worker = listenerThreads.get(0);
        int runnableSamples = 0, waitingSamples = 0;
        for (int i = 0; i < 10; i++) {
            Thread.State st = worker.getState();
            if (st == Thread.State.RUNNABLE) {
                runnableSamples++;
            }
            if (st == Thread.State.TIMED_WAITING || st == Thread.State.WAITING) {
                waitingSamples++;
            }
            Thread.sleep(8);
        }
        log.info("[case=noSpinCpu] workerStates samples: WAITING={} RUNNABLE={} "
                + "判定依据=退避窗口内线程绝大多数时间在等待而非运行", waitingSamples, runnableSamples);
        assertTrue(waitingSamples >= 8, "退避等待应阻塞线程，实际等待采样=" + waitingSamples);
        assertTrue(runnableSamples <= 1, "退避期间不得忙等自旋，RUNNABLE采样=" + runnableSamples);
        await("dead", () -> stats().get("dead") == 1L);
    }

    @Test
    void backoffOfOneSubscriberDoesNotBlockAnother() throws Exception {
        // 慢订阅：永不确认，进入长退避
        CopyOnWriteArrayList<Long> slowTimes = new CopyOnWriteArrayList<>();
        Subscription slow = broker.subscribe(TopicFilter.of("robot/slow"),
                d -> slowTimes.add(System.nanoTime()));
        // 快订阅：同主题族但独立队列与派发线程，立即确认
        CopyOnWriteArrayList<Long> fastDeliveries = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/fast"), d -> {
            fastDeliveries.add(System.nanoTime());
            d.ack();
        });

        broker.publish("robot/slow", Map.of("v", 1)); // 触发慢订阅退避
        await("slow in backoff", () -> stats().get("backoffWaits") >= 1L);

        // 慢订阅退避期间，快订阅多条消息应在远小于一个退避周期内全部确认
        long t0 = System.nanoTime();
        int n = 5;
        for (int i = 0; i < n; i++) {
            broker.publish("robot/fast", Map.of("seq", i));
        }
        await("fast all acked", () -> stats().get("acked") == (long) n);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        log.info("[case=backoffIsolation] fastAckElapsedMs={} backoff={} 判定依据=快订阅不受慢订阅退避连累",
                elapsedMs, B0);
        assertTrue(elapsedMs < B0, "快订阅应在一个退避周期内完成，实际=" + elapsedMs);
        assertEquals(1, slowTimes.size(), "慢订阅仍停在第一次投递后的退避中");
        broker.unsubscribe(slow.id());
    }

    @Test
    void disconnectDuringBackoffRedeliversExactlyOnceAfterReconnect() throws Exception {
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> attempts.add(d.attempt()));

        broker.publish("robot/arm", Map.of("v", 1));
        await("first attempt", () -> attempts.size() == 1);
        // 在第 1 次确认超时后、退避（100ms）期间断连
        await("entered backoff", () -> stats().get("backoffWaits") >= 1L);
        broker.disconnect(sub.id());

        // 跨过 ackTimeout + 全部退避 + 余量：断连期间绝不能有第二次投递
        Thread.sleep(ACK_TIMEOUT + B0 + B0 * 2 + 250);
        assertEquals(1, attempts.size(), "断连期间退避结束也不得投递");

        broker.reconnect(sub.id());
        await("second attempt after reconnect", () -> attempts.size() == 2);
        long beforeThird = attempts.size();
        // 再断连一次（落在第二次确认超时后的退避中）：恢复后仍只补投一次，不重复
        await("second backoff", () -> stats().get("backoffWaits") >= 2L);
        broker.disconnect(sub.id());
        Thread.sleep(ACK_TIMEOUT + B0 + 150);
        assertEquals(beforeThird, attempts.size());
        broker.reconnect(sub.id());
        await("third attempt", () -> attempts.size() == 3);

        Map<String, Long> s = stats();
        log.info("[case=backoffDisconnect] attempts={} stats={} 判定依据=退避中断连不丢不重、恢复只补投一次",
                attempts, s);
        assertEquals(List.of(1, 2, 3), attempts);
        assertEquals(2L, s.get("backoffWaits"));
        assertEquals(0L, s.get("dropped"));
        broker.unsubscribe(sub.id());
        await("dropped after unsubscribe", () -> stats().get("dropped") == 1L);
    }

    @Test
    void unsubscribeDuringBackoffDropsDeterministicallyAndDoesNotArchive() throws Exception {
        CopyOnWriteArrayList<Delivery> got = new CopyOnWriteArrayList<>();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), got::add);
        broker.publish("robot/arm", Map.of("v", 1));
        await("entered backoff", () -> stats().get("backoffWaits") >= 1L);

        broker.unsubscribe(sub.id());
        // 等待超过剩余退避 + ackTimeout，确认移除后不会再有投递、不会进死信
        Thread.sleep(B0 * 4 + ACK_TIMEOUT + 200);

        Map<String, Long> s = stats();
        log.info("[case=backoffUnsubscribe] stats={} 判定依据=退避中移除确定性dropped、不重投不留存", s);
        assertEquals(1, got.size(), "移除后不得再投递");
        assertEquals(1L, s.get("dropped"));
        assertEquals(0L, s.get("dead"));
        assertEquals(0L, s.get("deadArchived"));
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }
}
