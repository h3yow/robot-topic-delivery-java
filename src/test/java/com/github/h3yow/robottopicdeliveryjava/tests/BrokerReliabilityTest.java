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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class BrokerReliabilityTest {

    private static final Logger log = LoggerFactory.getLogger(BrokerReliabilityTest.class);

    private MessageBroker broker;

    @BeforeEach
    void setUp() {
        // 小超时便于测试快速收敛：容量16 / 确认超时100ms / 最多重试2次
        broker = new MessageBroker(new BrokerConfig(16, 100, 2, 2), new MessageCodec());
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
            Thread.sleep(5);
        }
    }

    private Map<String, Long> stats() {
        return broker.stats().snapshot();
    }

    @Test
    void publishValidationRejectsWithDistinctReasons() {
        Subscription sub = broker.subscribe(TopicFilter.of("robot/#"), Delivery::ack);

        PublishReceipt r1 = broker.publish("a//b", Map.of("x", 1));
        assertFalse(r1.accepted());
        assertEquals(RejectReason.INVALID_TOPIC, r1.rejectReason());

        PublishReceipt r2 = broker.publish("robot/arm", null);
        assertFalse(r2.accepted());
        assertEquals(RejectReason.INVALID_PAYLOAD, r2.rejectReason());

        // 自引用对象导致 JSON 编码失败
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);
        PublishReceipt r3 = broker.publish("robot/arm", cyclic);
        assertFalse(r3.accepted());
        assertEquals(RejectReason.ENCODE_FAILURE, r3.rejectReason());

        // 同批后续合法消息不受影响
        PublishReceipt r4 = broker.publish("robot/arm", Map.of("pos", 1.0));
        assertTrue(r4.accepted());

        log.info("[case=publishValidation] 输入=[非法主题, null负载, 循环引用负载, 合法消息] "
                + "判定={},{},{} stats={}", r1.rejectReason(), r2.rejectReason(), r3.rejectReason(),
                r4.accepted() ? "accepted" : "rejected", stats());
        assertEquals(3L, stats().get("publishRejected"));
        assertEquals(1L, stats().get("published"));
    }

    @Test
    void ackFlowAndCounterConservation() throws Exception {
        broker.subscribe(TopicFilter.of("robot/arm/position"), Delivery::ack);
        broker.subscribe(TopicFilter.of("robot/#"), Delivery::ack);

        int n = 10;
        for (int i = 0; i < n; i++) {
            assertTrue(broker.publish("robot/arm/position", Map.of("seq", i)).accepted());
        }
        await("all acked", () -> stats().get("acked") == 2L * n);

        Map<String, Long> s = stats();
        log.info("[case=ackFlow] 输入={}条消息x2订阅 stats={} 判定依据=enqueued==acked(无重试无终止)", n, s);
        assertEquals(2L * n, s.get("enqueued"));
        assertEquals(2L * n, s.get("delivered"));
        assertEquals(2L * n, s.get("acked"));
        assertEquals(0L, s.get("retries"));
        assertEquals(0L, s.get("dead"));
        // 守恒：enqueued == acked + dead + dropped（已静默，无在途）
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }

    @Test
    void duplicateAckCountedOnceDelivered() throws Exception {
        CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/#"), received::add);

        broker.publish("robot/arm", Map.of("v", 1));
        await("delivered", () -> received.size() == 1);

        Delivery d = received.get(0);
        d.ack();
        d.ack(); // 重复确认
        d.ack(); // 再次重复
        await("acked", () -> stats().get("acked") == 1L);
        await("duplicates counted", () -> stats().get("duplicateAcks") == 2L);

        Map<String, Long> s = stats();
        log.info("[case=duplicateAck] 输入=同一delivery确认3次 stats={} 判定依据=acked==1且duplicateAcks==2且不重投", s);
        assertEquals(1L, s.get("delivered")); // 未发生重复投递
        assertEquals(1L, s.get("acked"));
        assertEquals(2L, s.get("duplicateAcks"));
    }

    @Test
    void staleAndLateAcksDoNotResurrect() throws Exception {
        CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/arm"), received::add);

        broker.publish("robot/arm", Map.of("v", 1));

        // 第一次投递不确认，等超时重试到第2次投递
        await("first delivery", () -> received.size() == 1);
        await("retry delivered", () -> received.size() == 2);
        Delivery stale = received.get(0);   // attempt=1 的句柄
        Delivery current = received.get(1); // attempt=2 的句柄
        assertEquals(1, stale.attempt());
        assertEquals(2, current.attempt());

        stale.ack(); // 乱序/迟到的旧 attempt 确认：不得改变当前 IN_FLIGHT 状态
        await("stale ack counted", () -> stats().get("lateAcks") == 1L);
        assertEquals(0L, stats().get("acked"));

        current.ack(); // 当前 attempt 确认生效
        await("acked", () -> stats().get("acked") == 1L);

        // 再发布一条永不确认的消息，等其进入 DEAD 后确认 -> lateAck
        CopyOnWriteArrayList<Delivery> received2 = new CopyOnWriteArrayList<>();
        Subscription sub2 = broker.subscribe(TopicFilter.of("robot/leg/#"), d -> received2.add(d));
        broker.publish("robot/leg/knee", Map.of("v", 2));
        await("dead", () -> stats().get("dead") == 1L);
        assertEquals(3, received2.size()); // 1 + maxRetries(2) 次投递
        received2.get(received2.size() - 1).ack(); // 已终止后的迟到确认
        await("late ack counted", () -> stats().get("lateAcks") == 2L);

        Map<String, Long> s = stats();
        log.info("[case=staleAndLateAck] 输入=[旧attempt确认, 当前确认, DEAD后确认] stats={} "
                + "判定依据=状态不倒退、计数单调、DEAD不复活", s);
        assertEquals(1L, s.get("acked"));
        assertEquals(1L, s.get("dead"));
        assertEquals(2L, s.get("lateAcks"));
        assertEquals(3L, s.get("retries")); // 第一条消息重试1次 + 第二条重试2次
        broker.unsubscribe(sub2.id());
    }

    @Test
    void retryExhaustionLeadsToDead() throws Exception {
        CopyOnWriteArrayList<Delivery> received = new CopyOnWriteArrayList<>();
        broker.subscribe(TopicFilter.of("robot/#"), received::add); // 永不确认

        broker.publish("robot/arm", Map.of("v", 1));
        await("dead", () -> stats().get("dead") == 1L);

        Map<String, Long> s = stats();
        log.info("[case=retryExhaustion] 输入=不确认的订阅者 stats={} "
                + "判定依据=投递1+maxRetries次后进入DEAD终态", s);
        assertEquals(3L, s.get("delivered")); // 1 + 2 次重试
        assertEquals(3L, s.get("timeouts"));
        assertEquals(2L, s.get("retries"));
        assertEquals(1L, s.get("dead"));
        assertEquals(0L, s.get("acked"));
        assertEquals(s.get("enqueued"), s.get("acked") + s.get("dead") + s.get("dropped"));
    }
}
