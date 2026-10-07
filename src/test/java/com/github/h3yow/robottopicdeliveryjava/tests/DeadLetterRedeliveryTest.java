package com.github.h3yow.robottopicdeliveryjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败留存与重投能力测试：
 * - 重试耗尽的消息完整留存（主题/负载/订阅/投递次数/终结时间）；
 * - 留存->重投的完整链路：全新投递、旧记录不复活、不重复生效、计数对得上账；
 * - 无效/重复重投被以可区分原因挡下；
 * - 重投撞上目标队列饱和时沿用确定性背压拒绝；
 * - 留存容量溢出（EVICT_OLDEST / REJECT_NEW）的取舍与记账。
 */
class DeadLetterRedeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRedeliveryTest.class);

    private MessageBroker broker;

    @BeforeEach
    void setUp() {
        // 容量16 / 确认超时80ms / 最多重试2次 / 退避0(立即重试，专注留存与重投)
        broker = new MessageBroker(
                new BrokerConfig(16, 80, 2, 2, 0L, 1.0, 0L, 256, DeadLetterOverflowPolicy.EVICT_OLDEST),
                new MessageCodec());
    }

    @AfterEach
    void tearDown() {
        broker.close();
    }

    static void await(String what, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
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
    void exhaustedMessageIsFullyArchivedAndListable() throws Exception {
        CopyOnWriteArrayList<Delivery> got = new CopyOnWriteArrayList<>();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/arm"), got::add);

        broker.publish("robot/arm", Map.of("joint", "wrist", "angle", 42));
        await("dead archived", () -> stats().get("dead") == 1L);
        await("exactly one retained record", () -> broker.listDeadMessages(sub.id()).size() == 1);
        DeadMessage d = broker.listDeadMessages(sub.id()).get(0);

        assertEquals("robot/arm", d.topic().name());
        assertEquals(sub.id(), d.subscriptionId());
        assertEquals(3, d.attempts(), "总投递次数=1+maxRetries=3");
        assertEquals(DeadMessageStatus.RETAINED, d.status());
        assertNotNull(d.terminatedAt());
        assertNotNull(d.recordId());
        assertEquals(d.messageId(), d.originMessageId(), "非重投链路 origin==messageId");
        assertNull(d.redeliveredAsId());
        JsonNode payload = new MessageCodec().decode(d.payload());
        assertEquals("wrist", payload.get("joint").asText());
        assertEquals(42, payload.get("angle").asInt());

        // payload 为防御性拷贝，外部修改不影响留存内容
        d.payload()[0] = (byte) 0xFF;
        assertEquals("wrist", new MessageCodec()
                .decode(broker.deadMessage(d.recordId()).payload()).get("joint").asText());

        Map<String, Long> s = stats();
        log.info("[case=deadArchived] record={} attempts={} stats={} 判定依据=死信字段完整且已留存计数",
                d.recordId(), d.attempts(), s);
        assertEquals(1L, s.get("dead"));
        assertEquals(1L, s.get("deadArchived"));
        assertEquals(0L, s.get("deadArchiveRejected"));
        assertEquals(0L, s.get("deadEvicted"));
        assertEquals(1L, broker.diagnostics().get(sub.id()).get("deadRetained"));
    }

    @Test
    void redriveRunsAsBrandNewDeliveryAndDoesNotDuplicate() throws Exception {
        CopyOnWriteArrayList<Delivery> all = new CopyOnWriteArrayList<>();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), all::add);

        broker.publish("robot/arm", Map.of("v", 7));
        await("dead archived", () -> stats().get("dead") == 1L);
        DeadMessage dead = broker.listDeadMessages(sub.id()).get(0);
        String origId = dead.messageId();
        await("first run delivered 3 times", () -> all.size() == 3);

        RedeliveryResult rr = broker.redeliverDeadMessage(sub.id(), origId);
        assertTrue(rr.accepted(), () -> "redrive should be accepted: " + rr.rejectReason());
        assertNotNull(rr.newMessageId());
        assertNotEquals(origId, rr.newMessageId(), "重投必须是全新消息 id");
        assertEquals(dead.recordId(), rr.recordId());

        await("redriven record reflected",
                () -> broker.deadMessage(dead.recordId()).status() == DeadMessageStatus.REDRIVEN);
        DeadMessage after = broker.deadMessage(dead.recordId());
        assertEquals(DeadMessageStatus.REDRIVEN, after.status());
        assertEquals(rr.newMessageId(), after.redeliveredAsId());

        await("redrive delivered", () -> all.stream()
                .anyMatch(x -> x.message().id().equals(rr.newMessageId()) && x.attempt() == 1));
        Delivery freshDelivery = all.stream()
                .filter(x -> x.message().id().equals(rr.newMessageId()) && x.attempt() == 1)
                .findFirst().orElseThrow();
        assertEquals(origId, freshDelivery.message().originId(), "重投消息沿用原始链路 originId");
        freshDelivery.ack(); // 对全新投递的确认立即生效（此时正 IN_FLIGHT 等确认）

        await("one acked overall", () -> stats().get("acked") == 1L);
        Map<String, Long> s = stats();
        log.info("[case=redrive] orig={} new={} stats={} 判定依据=旧消息仍dead、新消息acked、只生效一次",
                origId, rr.newMessageId(), s);
        assertEquals(1L, s.get("dead"), "旧终结消息不复活");
        assertEquals(1L, s.get("acked"), "全新投递恰好生效一次");
        assertEquals(1L, s.get("redriveAccepted"));
        assertEquals(1L, s.get("reEnqueued"));
        assertEquals(2L, s.get("enqueued"), "首次1 + 重投1");
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }

    @Test
    void redriveRejectionsAreDistinct() throws Exception {
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), Delivery::ack);

        // 1) 订阅不存在
        RedeliveryResult gone = broker.redeliverDeadMessage("no-such-sub", "any-msg");
        assertFalse(gone.accepted());
        assertEquals(RedeliveryRejectReason.SUBSCRIPTION_GONE, gone.rejectReason());

        // 2) 消息已正常确认、该订阅下无留存记录
        PublishReceipt pr = broker.publish("robot/arm", Map.of("v", 1));
        String ackedId = pr.messageId();
        await("first message acked", () -> stats().get("acked") == 1L);
        RedeliveryResult notFound = broker.redeliverDeadMessage(sub.id(), ackedId);
        assertEquals(RedeliveryRejectReason.RECORD_NOT_FOUND, notFound.rejectReason());

        // 3) 仍在投递路上：永不确认的订阅，终结前重投
        CopyOnWriteArrayList<Delivery> inflight = new CopyOnWriteArrayList<>();
        Subscription slow = broker.subscribe(TopicFilter.of("robot/leg/#"), inflight::add);
        PublishReceipt pr2 = broker.publish("robot/leg/knee", Map.of("v", 2));
        await("in flight delivered once", () -> inflight.size() == 1);
        RedeliveryResult flying = broker.redeliverDeadMessage(slow.id(), pr2.messageId());
        assertEquals(RedeliveryRejectReason.STILL_IN_FLIGHT, flying.rejectReason());

        // 4) 终结后重投成功，再次重投 -> ALREADY_REDELIVERED
        await("second message dead", () -> stats().get("dead") >= 1L);
        DeadMessage dead = broker.listDeadMessages(slow.id()).get(0);
        RedeliveryResult ok = broker.redeliverDeadMessage(slow.id(), dead.messageId());
        assertTrue(ok.accepted());
        RedeliveryResult again = broker.redeliverDeadMessage(slow.id(), dead.messageId());
        assertEquals(RedeliveryRejectReason.ALREADY_REDELIVERED, again.rejectReason());
        assertEquals(dead.recordId(), again.recordId());

        Map<String, Long> s = stats();
        log.info("[case=redriveReject] reasons={},{},{},{} stats={} 判定依据=四类挡下原因可区分且各记账",
                gone.rejectReason(), notFound.rejectReason(), flying.rejectReason(),
                again.rejectReason(), s);
        assertEquals(4L, s.get("redriveRejected"));
        assertEquals(1L, s.get("redriveAccepted"));
        assertEquals(5L, s.get("redriveRequested"));

        broker.unsubscribe(slow.id());
    }

    @Test
    void redriveIntoSaturatedQueueIsRejectedAndRetryable() throws Exception {
        try (MessageBroker small = new MessageBroker(
                // 监听器阻塞在 latch 期间根本不会进入 ack 超时等待，故 80ms 超时只影响立即返回的死信消息
                new BrokerConfig(2, 80, 0, 2, 0L, 1.0, 0L, 256, DeadLetterOverflowPolicy.EVICT_OLDEST),
                new MessageCodec())) {
            CountDownLatch block = new CountDownLatch(1);
            CopyOnWriteArrayList<Delivery> fills = new CopyOnWriteArrayList<>();
            // 同一个订阅：带 "dead" 标记的消息永不确认（产生死信）；带 "fill" 的消息阻塞占满队列
            Subscription slow = small.subscribe(TopicFilter.of("robot/#"), d -> {
                JsonNode node = new MessageCodec().decode(d.message().payload());
                if (node.has("dead")) {
                    return; // 永不确认 -> 走完 1+0 次投递后终结留存
                }
                fills.add(d);
                try {
                    block.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                d.ack();
            });

            small.publish("robot/arm", Map.of("dead", 1));
            await("dead produced", () -> small.stats().snapshot().get("dead") == 1L);
            DeadMessage dead = small.listDeadMessages(slow.id()).get(0);

            // 确定性占满 slow（队列容量2）：先让 fill#0 成为在途（腾出队列槽位），再入队两条
            PublishReceipt f0 = small.publish("robot/arm", Map.of("fill", 0));
            assertTrue(f0.enqueueResults().get(0).enqueued());
            await("fill#0 inflight", () -> fills.size() == 1);
            for (int i = 1; i <= 2; i++) {
                PublishReceipt r = small.publish("robot/arm", Map.of("fill", i));
                assertTrue(r.enqueueResults().get(0).enqueued());
            }
            await("slow saturated: 1 inflight + 2 queued",
                    () -> fills.size() == 1
                            && small.diagnostics().get(slow.id()).get("queueDepth") == 2L);

            // 重投撞队列满 -> QUEUE_FULL，记录保持 RETAINED 可再试
            RedeliveryResult full = small.redeliverDeadMessage(slow.id(), dead.messageId());
            assertFalse(full.accepted());
            assertEquals(RedeliveryRejectReason.QUEUE_FULL, full.rejectReason());
            assertEquals(dead.recordId(), full.recordId());
            assertEquals(DeadMessageStatus.RETAINED,
                    small.deadMessage(dead.recordId()).status(), "队列满不得把记录标记为已重投");

            // 放行占满的消息后，重投成功且记录转 REDRIVEN
            block.countDown();
            await("slow drained", () -> fills.size() == 3);
            RedeliveryResult ok = small.redeliverDeadMessage(slow.id(), dead.messageId());
            assertTrue(ok.accepted(), () -> "retry redrive after drain: " + ok.rejectReason());
            assertEquals(DeadMessageStatus.REDRIVEN,
                    small.deadMessage(dead.recordId()).status());

            Map<String, Long> s = small.stats().snapshot();
            log.info("[case=redriveQueueFull] stats={} 判定依据=撞满被QUEUE_FULL挡下、记账、不吞、腾退后成功", s);
            assertEquals(1L, s.get("enqueueRejected"), "唯一一次入队拒绝就是重投撞满");
            assertEquals(1L, s.get("redriveAccepted"));
            assertEquals(1L, s.get("redriveRejected"));
            assertEquals(1L, s.get("reEnqueued"));
        }
    }

    @Test
    void retentionOverflowEvictsOldestWithAccounting() throws Exception {
        try (MessageBroker b = new MessageBroker(
                new BrokerConfig(16, 40, 0, 2, 0L, 1.0, 0L, 3, DeadLetterOverflowPolicy.EVICT_OLDEST),
                new MessageCodec())) {
            Subscription sub = b.subscribe(TopicFilter.of("robot/#"), d -> { });
            for (int i = 0; i < 5; i++) {
                b.publish("robot/arm", Map.of("seq", i));
            }
            await("5 dead produced", () -> b.stats().snapshot().get("dead") == 5L);

            List<DeadMessage> kept = b.listDeadMessages();
            assertEquals(3, kept.size(), "留存不超过容量上限");
            MessageCodec codec = new MessageCodec();
            List<Integer> seqs = kept.stream()
                    .map(d -> codec.decode(d.payload()).get("seq").asInt()).sorted().toList();
            assertEquals(List.of(2, 3, 4), seqs, "淘汰最旧的两条，保留最新三条");
            // 按 (sub,msg) 查不到被淘汰记录
            assertNull(b.listDeadMessages(sub.id()).stream()
                    .filter(d -> codec.decode(d.payload()).get("seq").asInt() < 2).findFirst()
                    .orElse(null));

            Map<String, Long> s = b.stats().snapshot();
            log.info("[case=evictOldest] keptSeqs={} stats={} 判定依据=dead==archived+rejected且淘汰2条最旧",
                    seqs, s);
            assertEquals(5L, s.get("dead"));
            assertEquals(5L, s.get("deadArchived"));
            assertEquals(2L, s.get("deadEvicted"));
            assertEquals(0L, s.get("deadArchiveRejected"));
            assertEquals(s.get("dead"), s.get("deadArchived") + s.get("deadArchiveRejected"));
        }
    }

    @Test
    void retentionOverflowRejectNewKeepsExisting() throws Exception {
        try (MessageBroker b = new MessageBroker(
                new BrokerConfig(16, 40, 0, 2, 0L, 1.0, 0L, 2, DeadLetterOverflowPolicy.REJECT_NEW),
                new MessageCodec())) {
            b.subscribe(TopicFilter.of("robot/#"), d -> { });
            for (int i = 0; i < 4; i++) {
                b.publish("robot/arm", Map.of("seq", i));
            }
            await("4 dead produced", () -> b.stats().snapshot().get("dead") == 4L);

            List<DeadMessage> kept = b.listDeadMessages();
            assertEquals(2, kept.size());
            MessageCodec codec = new MessageCodec();
            List<Integer> seqs = kept.stream()
                    .map(d -> codec.decode(d.payload()).get("seq").asInt()).sorted().toList();
            assertEquals(List.of(0, 1), seqs, "REJECT_NEW 保留最早的记录");

            Map<String, Long> s = b.stats().snapshot();
            log.info("[case=rejectNew] keptSeqs={} stats={} 判定依据=新终结不留存且记账、旧记录不动", seqs, s);
            assertEquals(4L, s.get("dead"));
            assertEquals(2L, s.get("deadArchived"));
            assertEquals(2L, s.get("deadArchiveRejected"));
            assertEquals(0L, s.get("deadEvicted"));
            assertEquals(s.get("dead"), s.get("deadArchived") + s.get("deadArchiveRejected"));
        }
    }

    @Test
    void concurrentRedriveOfSameRecordAcceptedExactlyOnce() throws Exception {
        CopyOnWriteArrayList<Delivery> got = new CopyOnWriteArrayList<>();
        String[] originalId = new String[1];
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), d -> {
            got.add(d);
            // 首轮原消息永不确认（产生死信）；重投产生的新消息（id 不同）立即确认
            if (originalId[0] != null && !d.message().id().equals(originalId[0])) {
                d.ack();
            }
        });
        PublishReceipt pr = broker.publish("robot/arm", Map.of("v", 1));
        originalId[0] = pr.messageId();
        await("dead", () -> stats().get("dead") == 1L);
        DeadMessage dead = broker.listDeadMessages(sub.id()).get(0);

        int threads = 32;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threads);
        List<RedeliveryResult> results = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(broker.redeliverDeadMessage(sub.id(), dead.messageId()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, java.util.concurrent.TimeUnit.SECONDS));
        pool.shutdown();

        long accepted = results.stream().filter(RedeliveryResult::accepted).count();
        long already = results.stream()
                .filter(r -> r.rejectReason() == RedeliveryRejectReason.ALREADY_REDELIVERED).count();
        String newId = results.stream().filter(RedeliveryResult::accepted)
                .map(RedeliveryResult::newMessageId).findFirst().orElseThrow();
        await("redriven message acked", () -> stats().get("acked") == 1L);

        Map<String, Long> s = stats();
        log.info("[case=concurrentRedrive] accepted={} alreadyRedelivered={} stats={} "
                + "判定依据=并发32次重投仅1次入队生效、其余以ALREADY_REDELIVERED挡下", accepted, already, s);
        assertEquals(1, accepted, "同一条死信并发重投只能有一次被接受");
        assertEquals(threads - 1, already, "其余全部以 ALREADY_REDELIVERED 挡下");
        assertEquals(1L, s.get("redriveAccepted"));
        assertEquals(threads - 1L, s.get("redriveRejected"));
        assertEquals(threads, s.get("redriveRequested"));
        assertEquals(2L, s.get("enqueued"), "首次1 + 重投1，绝不重复入队");
        assertEquals(1L, s.get("reEnqueued"));
        // 新消息对监听器只生效一次（按重投产生的新消息 id 精确计数）
        long freshDeliveries = got.stream().filter(d -> d.message().id().equals(newId)).count();
        assertEquals(1L, freshDeliveries, "重投消息只投递/生效一次");
    }

    @Test
    void redrivenMessageCanDeadAgainAndChainIsTraceable() throws Exception {
        CopyOnWriteArrayList<Delivery> got = new CopyOnWriteArrayList<>();
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), got::add); // 永不确认
        broker.publish("robot/arm", Map.of("v", 1));
        await("first dead", () -> stats().get("dead") == 1L);
        DeadMessage first = broker.listDeadMessages(sub.id()).get(0);
        String rootOrigin = first.originMessageId();

        RedeliveryResult rr = broker.redeliverDeadMessage(sub.id(), first.messageId());
        assertTrue(rr.accepted());
        await("second dead", () -> stats().get("dead") == 2L);

        List<DeadMessage> all = broker.listDeadMessages(sub.id());
        assertEquals(2, all.size());
        DeadMessage second = all.stream()
                .filter(d -> d.messageId().equals(rr.newMessageId())).findFirst().orElseThrow();
        assertEquals(rootOrigin, second.originMessageId(), "重投再终结仍可追溯同一源头");
        assertEquals(DeadMessageStatus.REDRIVEN,
                broker.deadMessage(first.recordId()).status(), "旧记录已标记 REDRIVEN");
        assertEquals(DeadMessageStatus.RETAINED, second.status());

        Map<String, Long> s = stats();
        log.info("[case=redriveChain] stats={} 判定依据=两批入队守恒、链路origin一致", s);
        assertEquals(2L, s.get("enqueued"));
        assertEquals(2L, s.get("dead"));
        assertEquals(2L, s.get("deadArchived"));
        assertEquals(1L, s.get("redriveAccepted"));
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }
}
