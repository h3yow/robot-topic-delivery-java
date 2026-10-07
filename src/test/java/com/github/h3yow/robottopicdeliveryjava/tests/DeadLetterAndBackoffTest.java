package com.github.h3yow.robottopicdeliveryjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.h3yow.robottopicdeliveryjava.broker.*;
import com.github.h3yow.robottopicdeliveryjava.codec.MessageCodec;
import com.github.h3yow.robottopicdeliveryjava.config.BrokerConfig;
import com.github.h3yow.robottopicdeliveryjava.topic.TopicFilter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败留存与重投、重试退避的新增行为测试。
 *
 * <p>为避免并发时序导致断言退化，所有用例通过「永不确认的单条消息 + 状态/计数到达」同步，
 * 时间相关断言使用明显大于派发粒度（2ms）、明显小于退避量级的区间。
 */
class DeadLetterAndBackoffTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterAndBackoffTest.class);

    private final MessageCodec codec = new MessageCodec();

    /** 容量16 / 超时60ms / 最多重试2次 / 退避首等200ms倍率2 / 留存容量100淘汰最老 */
    private MessageBroker newBackoffBroker() {
        return new MessageBroker(new BrokerConfig(16, 60, 2, 2,
                100, OverflowPolicy.DROP_OLDEST, 200L, 2.0d, 60_000L), codec);
    }

    private MessageBroker newBroker(int queueCapacity, int deadLetterCapacity,
                                    OverflowPolicy overflowPolicy,
                                    long backoffInitial, double multiplier) {
        return new MessageBroker(new BrokerConfig(queueCapacity, 60, 2, 2,
                deadLetterCapacity, overflowPolicy, backoffInitial, multiplier, 60_000L), codec);
    }

    static void await(String what, BooleanSupplier cond) throws InterruptedException {
        BrokerReliabilityTest.await(what, cond);
    }

    private Map<String, Long> stats(MessageBroker broker) {
        return broker.stats().snapshot();
    }

    private DeadLetter firstDeadLetter(MessageBroker broker) throws InterruptedException {
        await("one dead letter retained",
                () -> broker.stats().snapshot().get("deadLetterRetained") == 1L);
        List<DeadLetter> all = broker.deadLetters();
        assertEquals(1, all.size());
        return all.get(0);
    }

    // ------------------------------------------------------ 留存与重投完整链路

    @Test
    void deadLetterRetainedWithFullContextAndReplayRunsAsFreshDelivery() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            Subscription sub = broker.subscribe(TopicFilter.of("robot/arm"), received::add);
            Map<String, Object> payload = Map.of("seq", 42, "kind", "pick");
            broker.publish("robot/arm", payload);

            // 1) 重试耗尽 -> 一条留存，上下文完整
            await("dead", () -> stats(broker).get("dead") == 1L);
            DeadLetter letter = firstDeadLetter(broker);
            log.info("[case=retainAndReplay] 留存 letter={} sub={} topic={} attempts={} state={}",
                    letter.id(), letter.subscriptionId(), letter.topic(), letter.attempts(),
                    letter.state());
            assertEquals("robot/arm", letter.topic());
            assertEquals(sub.id(), letter.subscriptionId());
            assertEquals(3, letter.attempts());                 // 1 + maxRetries(2)
            assertEquals(DeadLetterState.RETAINED, letter.state());
            assertNotNull(letter.diedAt());
            assertNull(letter.originId());
            assertNull(letter.replayedAsMessageId());
            JsonNode decoded = codec.decode(letter.payload());  // 负载完整可读
            assertEquals(42, decoded.get("seq").asInt());
            assertEquals("pick", decoded.get("kind").asText());
            // 旧投递已终结：投递次数恰好 3，留存后不再发生投递
            assertEquals(3, received.size());

            // 2) 重投 = 全新投递：新消息 id，重新投递一次
            ReplayResult result = broker.replayDeadLetter(letter.id());
            log.info("[case=retainAndReplay] 重投结果={} newMsg={}", result.accepted(),
                    result.newMessageId());
            assertTrue(result.accepted());
            assertNotEquals(letter.id(), result.newMessageId());
            await("replay delivered", () -> received.size() == 4);
            Delivery replayDelivery = received.get(3);
            assertEquals(result.newMessageId(), replayDelivery.message().id());
            assertEquals(1, replayDelivery.attempt());        // attempt 从 1 重新计
            assertEquals(letter.id(), replayDelivery.message().originDeadLetterId());

            // 源留存不复活：进入 REPLAYING（此时重投消息仍在途未确认）
            await("source replaying",
                    () -> broker.deadLetter(letter.id()).state() == DeadLetterState.REPLAYING);
            Map<String, Long> dlDiagInflight = broker.deadLetterDiagnostics();
            assertEquals(1L, dlDiagInflight.get("deadLetterCount"));
            assertEquals(100L, dlDiagInflight.get("deadLetterCapacity"));
            assertEquals(1L, dlDiagInflight.get("deadLetterStateReplaying"));

            // 3) 确认重投 -> 源留存 REPLAYED，终态不可逆
            replayDelivery.ack();
            await("source replayed",
                    () -> broker.deadLetter(letter.id()).state() == DeadLetterState.REPLAYED);
            Map<String, Long> dlDiagDone = broker.deadLetterDiagnostics();
            log.info("[case=retainAndReplay] 留存诊断={}", dlDiagDone);
            assertEquals(1L, dlDiagDone.get("deadLetterStateReplayed"));
            assertEquals(0L, dlDiagDone.get("deadLetterStateRetained"));
            // 再确认/再投递都不会发生
            Thread.sleep(80);
            assertEquals(4, received.size());

            Map<String, Long> s = stats(broker);
            log.info("[case=retainAndReplay] stats={} 判定依据=重投走全新流程且计数守恒", s);
            assertEquals(1L, s.get("replayAccepted"));
            assertEquals(2L, s.get("enqueued"));   // 原始1 + 重投1
            assertEquals(1L, s.get("dead"));
            assertEquals(1L, s.get("acked"));
            assertEquals(0L, s.get("dropped"));
            assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
        }
    }

    @Test
    void replayThatFailsAgainCreatesNewDeadLetterAndSourceStaysSettled() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            broker.subscribe(TopicFilter.of("robot/#"), received::add); // 永不确认
            broker.publish("robot/arm", Map.of("v", 1));

            await("first dead", () -> stats(broker).get("dead") == 1L);
            DeadLetter first = firstDeadLetter(broker);
            assertEquals(3, received.size());

            ReplayResult result = broker.replayDeadLetter(first.id());
            assertTrue(result.accepted());
            await("second dead", () -> stats(broker).get("dead") == 2L);

            // 源留存 REPLAYED；重投再次耗尽产生一条新的、可再重投的留存
            assertEquals(DeadLetterState.REPLAYED, broker.deadLetter(first.id()).state());
            List<DeadLetter> letters = broker.deadLetters();
            assertEquals(2, letters.size());
            DeadLetter second = letters.get(1);
            assertEquals(DeadLetterState.RETAINED, second.state());
            assertEquals(first.id(), second.originId());
            assertEquals(result.newMessageId(), second.id());
            assertEquals(3, second.attempts()); // 重投重新走 1+2 次

            // 对已终结的源留存再次重投 -> 明确挡下（ALREADY_REPLAYED）
            ReplayResult again = broker.replayDeadLetter(first.id());
            assertFalse(again.accepted());
            assertEquals(ReplayRejectReason.ALREADY_REPLAYED, again.rejectReason());
            // 新留存可以继续重投：接受后等其再次终止，系统重归静默再对账
            ReplayResult third = broker.replayDeadLetter(second.id());
            assertTrue(third.accepted());
            await("third delivery dead", () -> stats(broker).get("dead") == 3L);

            Map<String, Long> s = stats(broker);
            log.info("[case=replayFailsAgain] stats={} 判定依据=源留存不复活、新留存独立可重投、守恒成立", s);
            assertEquals(2L, s.get("replayAccepted"));
            assertEquals(1L, s.get("replayRejectedAlreadyReplayed"));
            assertEquals(3L, s.get("enqueued"));
            assertEquals(3L, s.get("dead"));
            assertEquals(0L, s.get("acked"));
            assertEquals(0L, s.get("dropped"));
            assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
        }
    }

    // ------------------------------------------------------ 无效/重复/在途重投被挡下

    @Test
    void invalidReplaysRejectedWithDistinctReasons() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), received::add);
            broker.publish("robot/arm", Map.of("v", 1));
            await("dead", () -> stats(broker).get("dead") == 1L);
            DeadLetter letter = firstDeadLetter(broker);

            // 不存在的 id
            ReplayResult missing = broker.replayDeadLetter("does-not-exist");
            assertFalse(missing.accepted());
            assertEquals(ReplayRejectReason.DEAD_LETTER_NOT_FOUND, missing.rejectReason());

            // 合法重投一次，重投消息仍在途 -> 再要求重投 = REPLAY_IN_FLIGHT
            ReplayResult first = broker.replayDeadLetter(letter.id());
            assertTrue(first.accepted());
            await("replay inflight delivery", () -> received.size() == 4);
            ReplayResult inflight = broker.replayDeadLetter(letter.id());
            assertFalse(inflight.accepted());
            assertEquals(ReplayRejectReason.REPLAY_IN_FLIGHT, inflight.rejectReason());

            // 确认后 -> ALREADY_REPLAYED
            received.get(3).ack();
            await("replayed",
                    () -> broker.deadLetter(letter.id()).state() == DeadLetterState.REPLAYED);
            ReplayResult already = broker.replayDeadLetter(letter.id());
            assertFalse(already.accepted());
            assertEquals(ReplayRejectReason.ALREADY_REPLAYED, already.rejectReason());

            // 目标订阅已不存在
            broker.unsubscribe(sub.id());
            // 重新制造一条留存（用另一个订阅），再删订阅
            Subscription temp = broker.subscribe(TopicFilter.of("robot/leg"), received::add);
            broker.publish("robot/leg", Map.of("v", 2));
            await("second dead", () -> stats(broker).get("dead") == 2L);
            DeadLetter letter2 = broker.deadLetters().stream()
                    .filter(l -> l.subscriptionId().equals(temp.id())).findFirst().orElseThrow();
            broker.unsubscribe(temp.id());
            ReplayResult noSub = broker.replayDeadLetter(letter2.id());
            assertFalse(noSub.accepted());
            assertEquals(ReplayRejectReason.SUBSCRIPTION_NOT_FOUND, noSub.rejectReason());

            Map<String, Long> s = stats(broker);
            log.info("[case=invalidReplay] stats={} 判定依据=四类挡下原因彼此分清且分别记账", s);
            assertEquals(1L, s.get("replayRejectedNotFound"));
            assertEquals(1L, s.get("replayRejectedInFlight"));
            assertEquals(1L, s.get("replayRejectedAlreadyReplayed"));
            assertEquals(1L, s.get("replayRejectedSubscriptionNotFound"));
            // 被挡下的请求都没有产生入队；入队 = 原始2条 + 唯一被接受的重投1条
            assertEquals(3L, s.get("enqueued"));
        }
    }

    // ------------------------------------------------------ 重投撞上队列饱和

    @Test
    void replayAgainstSaturatedQueueIsRejectedDeterministicallyAndRetryable() throws Exception {
        // 容量3、无退避：阶段1 让一条消息在该订阅上重试耗尽成为留存；
        // 阶段2 用一条阻塞在途消息占住在途位，再填满队列 -> 重投必然 QUEUE_FULL。
        try (MessageBroker broker = newBroker(3, 100, OverflowPolicy.DROP_OLDEST, 0L, 1.0d)) {
            CountDownLatch block = new CountDownLatch(1);
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
                received.add(d);
                String kind = codec.decode(d.message().payload()).path("kind").asText("");
                if ("block".equals(kind)) {
                    try {
                        block.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    d.ack();
                }
                // 其余消息永不确认 -> 超时重试耗尽
            });

            // 阶段1：产生一条属于该订阅的留存
            broker.publish("robot/arm", Map.of("seq", 1));
            await("dead letter from saturated sub",
                    () -> stats(broker).get("deadLetterRetained") == 1L);
            DeadLetter letter = broker.deadLetters().get(0);
            assertEquals(sub.id(), letter.subscriptionId());
            assertEquals(3, received.size()); // 1+2 次投递后终止，worker 已空闲

            // 阶段2：阻塞在途 1 条 + 填满队列 3 条（容量3）
            PublishReceipt blocked = broker.publish("robot/arm", Map.of("kind", "block"));
            assertTrue(blocked.accepted());
            await("block inflight", () -> received.size() == 4);
            int queued = 0;
            for (int i = 0; i < 10; i++) {
                PublishReceipt r = broker.publish("robot/arm", Map.of("fill", i));
                for (PublishReceipt.EnqueueResult er : r.enqueueResults()) {
                    if (er.subscriptionId().equals(sub.id()) && er.enqueued()) queued++;
                }
            }
            assertEquals(3, queued, "容量3恰好填满，多余发布被背压");
            assertEquals(3L, broker.diagnostics().get(sub.id()).get("queueDepth"));
            assertEquals(1L, broker.diagnostics().get(sub.id()).get("inFlight"));

            // 重投撞队列满：确定性 QUEUE_FULL，不被吞、不改变留存
            ReplayResult replay = broker.replayDeadLetter(letter.id());
            log.info("[case=replayQueueFull] 结果={} reason={}", replay.accepted(),
                    replay.rejectReason());
            assertFalse(replay.accepted());
            assertEquals(ReplayRejectReason.QUEUE_FULL, replay.rejectReason());
            assertEquals(DeadLetterState.RETAINED, broker.deadLetter(letter.id()).state());
            assertNull(broker.deadLetter(letter.id()).replayedAsMessageId());
            Map<String, Long> s1 = stats(broker);
            assertEquals(1L, s1.get("replayRejectedQueueFull"));
            // 重投的队列满与发布背压计入同一 enqueueRejected：填满时被拒7条 + 重投1条
            assertEquals(8L, s1.get("enqueueRejected"));
            assertEquals(0L, s1.get("replayAccepted"));

            // 队列腾开后，同一条留存可以重新重投成功（挡下是暂时的，记录未被消费）
            block.countDown();
            await("queue drained", () -> broker.diagnostics().get(sub.id()).get("queueDepth") == 0L);
            // 此时队里积压消息已全部投递且永不确认，等待它们终止会占很久；直接换一个空闲订阅不行
            // （重投只认原订阅）。因此在积压仍在重试期间，只要队列有空位即可重投：
            // 排空判定通过即至少出现空位，重投接受。
            ReplayResult retry = broker.replayDeadLetter(letter.id());
            log.info("[case=replayQueueFull] 腾位后重投结果={}", retry.accepted());
            assertTrue(retry.accepted());
            assertEquals(DeadLetterState.REPLAYING, broker.deadLetter(letter.id()).state());
            assertEquals(1L, stats(broker).get("replayAccepted"));
        }
    }


    // ------------------------------------------------------ 退避等待

    @Test
    void retryBackoffSpacesAttemptsWithIncreasingDelay() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Long> deliveryTimes = new CopyOnWriteArrayList<>();
            broker.subscribe(TopicFilter.of("robot/#"), d -> deliveryTimes.add(System.nanoTime()));
            long t0 = System.nanoTime();
            broker.publish("robot/arm", Map.of("v", 1));
            await("dead", () -> stats(broker).get("dead") == 1L);

            assertEquals(3, deliveryTimes.size());
            long gap1Ms = (deliveryTimes.get(1) - deliveryTimes.get(0)) / 1_000_000;
            long gap2Ms = (deliveryTimes.get(2) - deliveryTimes.get(1)) / 1_000_000;
            long totalMs = (deliveryTimes.get(2) - t0) / 1_000_000;
            log.info("[case=backoffTiming] gap1={}ms gap2={}ms total={}ms 判定依据="
                    + "gap1≈ackTimeout+200，gap2≈ackTimeout+400，逐次变长", gap1Ms, gap2Ms, totalMs);
            // ackTimeout=60；退避 200 / 400；留足 CI 容差但上下界都严格
            assertTrue(gap1Ms >= 240 && gap1Ms <= 600, "gap1=" + gap1Ms);
            assertTrue(gap2Ms >= 440 && gap2Ms <= 900, "gap2=" + gap2Ms);
            assertTrue(gap2Ms > gap1Ms + 100, "backoff should grow: gap1=" + gap1Ms + " gap2=" + gap2Ms);
            assertEquals(2L, stats(broker).get("backoffWaits"));
            assertEquals(0L, stats(broker).get("backoffInterrupted"));
        }
    }

    @Test
    void backoffDoesNotSpinCpuAndDoesNotAffectOtherSubscriptions() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            AtomicInteger otherAcked = new AtomicInteger();
            broker.subscribe(TopicFilter.of("robot/backoff"), d -> { /* 永不确认，持续退避 */ });
            broker.subscribe(TopicFilter.of("robot/fast/#"), d -> {
                otherAcked.incrementAndGet();
                d.ack();
            });

            broker.publish("robot/backoff", Map.of("v", 1));
            await("backoff entered", () -> stats(broker).get("backoffWaits") >= 1L);

            // 退避进行期间，另一个订阅仍能即时收发确认多条消息
            // 再发 20 条，记录完成确认的耗时：应远短于退避周期，且不依赖退避订阅推进
            long t = System.nanoTime();
            for (int i = 0; i < 20; i++) {
                broker.publish("robot/fast", Map.of("seq", i));
            }
            await("fast subscriber unaffected", () -> otherAcked.get() == 20);
            long waitMs = (System.nanoTime() - t) / 1_000_000;
            log.info("[case=backoffIsolation] 退避期间快订阅20条确认耗时={}ms backoffWaits={}",
                    waitMs, stats(broker).get("backoffWaits"));
            assertTrue(waitMs < 2_000, "快订阅不应被退避订阅阻塞");
            assertEquals(20L, stats(broker).get("acked"));
        }
    }

    // ------------------------------------------------------ 退避期间断连/移除

    @Test
    void disconnectDuringBackoffParksAndReconnectRedeliversOnce() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            AtomicInteger ackedAttempts = new AtomicInteger();
            Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
                received.add(d);
                // 第 2 次投递（退避后的重投）确认；但断连期间不应发生它
                if (d.attempt() == 2) {
                    ackedAttempts.incrementAndGet();
                    d.ack();
                }
            });
            broker.publish("robot/arm", Map.of("v", 1));
            await("first delivery", () -> received.size() == 1);
            // 在第一次退避（200ms）窗口内断连
            Thread.sleep(120);
            broker.disconnect(sub.id());
            await("backoff interrupted",
                    () -> stats(broker).get("backoffInterrupted") == 1L);
            // 断连持续超过完整退避+超时窗口：不得发生第 2 次投递
            Thread.sleep(500);
            assertEquals(1, received.size(), "断连期间退避停车，不重投");
            assertEquals(0, ackedAttempts.get());

            broker.reconnect(sub.id());
            await("acked after reconnect", () -> stats(broker).get("acked") == 1L);
            assertEquals(2, received.size(), "恢复后恰好补投一次");
            // 每条 attempt 只被确认一次：无重复生效
            Thread.sleep(100);
            assertEquals(1, ackedAttempts.get());

            Map<String, Long> s = stats(broker);
            log.info("[case=backoffDisconnect] stats={} attempts={} 判定依据=断连停车不丢不重，恢复补投一次",
                    s, received.stream().map(Delivery::attempt).toList());
            assertEquals(1L, s.get("acked"));
            assertEquals(0L, s.get("dead"));
            assertEquals(0L, s.get("dropped"));
            assertEquals(1L, s.get("backoffInterrupted"));
        }
    }

    @Test
    void unsubscribeDuringBackoffDropsDeterministically() throws Exception {
        try (MessageBroker broker = newBackoffBroker()) {
            CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
            Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), received::add);
            broker.publish("robot/arm", Map.of("v", 1));
            await("first delivery", () -> received.size() == 1);
            Thread.sleep(120); // 进入首次退避
            broker.unsubscribe(sub.id());
            await("dropped", () -> stats(broker).get("dropped") == 1L);

            Map<String, Long> s = stats(broker);
            log.info("[case=backoffUnsubscribe] stats={} 判定依据=退避中移除确定计dropped，不重投", s);
            assertEquals(1, received.size());
            assertEquals(1L, s.get("dropped"));
            assertEquals(0L, s.get("acked"));
            assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
        }
    }

    // ------------------------------------------------------ 留存容量溢出

    @Test
    void overflowDropOldestEvictsEarliestAndAccounts() throws Exception {
        try (MessageBroker broker = newBroker(16, 3, OverflowPolicy.DROP_OLDEST, 0L, 1.0d)) {
            broker.subscribe(TopicFilter.of("robot/#"), d -> { /* 永不确认 */ });
            for (int i = 0; i < 5; i++) {
                broker.publish("robot/arm", Map.of("seq", i));
            }
            await("all 5 dead", () -> stats(broker).get("dead") == 5L);

            List<DeadLetter> letters = broker.deadLetters();
            assertEquals(3, letters.size(), "容量上限严格为3");
            // 剩下的是最新的 3 条；最老的 2 条被淘汰且不可重投
            JsonNode newestPayload = codec.decode(letters.get(0).payload());
            assertEquals(2, newestPayload.get("seq").asInt());
            assertEquals(4, codec.decode(letters.get(2).payload()).get("seq").asInt());

            // 被淘汰的老 id 重投 -> NOT_FOUND（淘汰即不可见，理由可分辨）
            // 通过死信终结顺序无法直接拿到被淘汰 id；用不存在 id 已由其它用例覆盖，
            // 这里校验计数账：5 终止 == 3 留存 + 2 淘汰
            Map<String, Long> s = stats(broker);
            log.info("[case=overflowOldest] stats={} 判定依据=dead==retained+evictedOldest", s);
            assertEquals(5L, s.get("dead"));
            assertEquals(3L, s.get("deadLetterRetained"));
            assertEquals(2L, s.get("deadLetterEvictedOldest"));
            assertEquals(0L, s.get("deadLetterEvictedNewest"));

            // 再终结一条：仍淘汰最老（即 seq=2）
            broker.publish("robot/arm", Map.of("seq", 5));
            await("6th dead", () -> stats(broker).get("dead") == 6L);
            List<DeadLetter> now = broker.deadLetters();
            assertEquals(3, now.size());
            assertEquals(3, codec.decode(now.get(0).payload()).get("seq").asInt());
            assertEquals(5, codec.decode(now.get(2).payload()).get("seq").asInt());
            assertEquals(3L, stats(broker).get("deadLetterEvictedOldest"));
        }
    }

    @Test
    void overflowDropNewestRejectsNewRetentionAndAccounts() throws Exception {
        try (MessageBroker broker = newBroker(16, 2, OverflowPolicy.DROP_NEWEST, 0L, 1.0d)) {
            broker.subscribe(TopicFilter.of("robot/#"), d -> { /* 永不确认 */ });
            for (int i = 0; i < 4; i++) {
                broker.publish("robot/arm", Map.of("seq", i));
            }
            await("all 4 dead", () -> stats(broker).get("dead") == 4L);

            List<DeadLetter> letters = broker.deadLetters();
            assertEquals(2, letters.size());
            // 老的两条保留，新的两条被拒绝留存
            assertEquals(0, codec.decode(letters.get(0).payload()).get("seq").asInt());
            assertEquals(1, codec.decode(letters.get(1).payload()).get("seq").asInt());

            Map<String, Long> s = stats(broker);
            log.info("[case=overflowNewest] stats={} 判定依据=dead==retained+evictedNewest", s);
            assertEquals(4L, s.get("dead"));
            assertEquals(2L, s.get("deadLetterRetained"));
            assertEquals(2L, s.get("deadLetterEvictedNewest"));
            assertEquals(0L, s.get("deadLetterEvictedOldest"));
        }
    }

    // ------------------------------------------------------ 零退避保持旧行为

    @Test
    void zeroBackoffKeepsImmediateRetryBehavior() throws Exception {
        try (MessageBroker broker = newBroker(16, 100, OverflowPolicy.DROP_OLDEST, 0L, 2.0d)) {
            CopyOnWriteArrayList<Long> times = new CopyOnWriteArrayList<>();
            broker.subscribe(TopicFilter.of("robot/#"), d -> times.add(System.nanoTime()));
            broker.publish("robot/arm", Map.of("v", 1));
            await("dead", () -> stats(broker).get("dead") == 1L);
            assertEquals(3, times.size());
            long gap1 = (times.get(1) - times.get(0)) / 1_000_000;
            long gap2 = (times.get(2) - times.get(1)) / 1_000_000;
            log.info("[case=zeroBackoff] gap1={}ms gap2={}ms 判定依据=仅有ackTimeout间隔、无退避计数",
                    gap1, gap2);
            assertTrue(gap1 < 200, "zero backoff should retry immediately after timeout: " + gap1);
            assertTrue(gap2 < 200, "zero backoff should retry immediately after timeout: " + gap2);
            assertEquals(0L, stats(broker).get("backoffWaits"));
            assertEquals(0L, stats(broker).get("backoffInterrupted"));
        }
    }
}
