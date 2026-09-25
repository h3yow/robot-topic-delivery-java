package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.*;
import com.github.h3yow.robottopicdeliveryjava.messaging.exception.EncodingFailureException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BrokerRejectionTest {

    private InMemoryMessageBroker broker;

    private InMemoryMessageBroker newBroker() {
        BrokerProperties properties = new BrokerProperties();
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
    void unknownTopicInvalidTopicPayloadAndEncodingHaveDistinguishableReasons() {
        InMemoryMessageBroker b = newBroker();
        b.registerTopic("robot/r1/status", String.class, true);

        PublishReceipt unknown = b.publish("robot/r1/missing", "x");
        assertEquals(PublishStatus.REJECTED, unknown.status());
        assertTrue(unknown.reason().startsWith("UNKNOWN_TOPIC"), unknown.reason());

        PublishReceipt badTopic = b.publish("robot//status", "x");
        assertTrue(badTopic.reason().startsWith("INVALID_TOPIC"), badTopic.reason());

        PublishReceipt wrongType = b.publish("robot/r1/status", 42);
        assertTrue(wrongType.reason().startsWith("INVALID_PAYLOAD"), wrongType.reason());

        PublishReceipt nullPayload = b.publish("robot/r1/status", null);
        assertTrue(nullPayload.reason().startsWith("INVALID_PAYLOAD"), nullPayload.reason());

        // 编码失败：伪造一个 Jackson 无法序列化的对象。
        MessageCodec failing = new MessageCodec() {
            @Override
            public byte[] encode(Object payload) {
                throw new EncodingFailureException("boom-encoding", null);
            }

            @Override
            public Object decode(byte[] data, Class<?> payloadType) {
                throw new UnsupportedOperationException();
            }
        };
        broker.close();
        InMemoryMessageBroker b2 = new InMemoryMessageBroker(new BrokerProperties(), failing);
        b2.registerTopic("t", String.class, false);
        PublishReceipt encFail = b2.publish("t", "anything");
        assertTrue(encFail.reason().startsWith("ENCODING_FAILURE"), encFail.reason());
        b2.close();
    }

    @Test
    void oneRejectedMessageInBatchDoesNotAffectOthers() throws Exception {
        InMemoryMessageBroker b = newBroker();
        b.registerTopic("ok", String.class, true);
        CountDownLatch got = new CountDownLatch(2);
        b.subscribe("s1", "ok", (m, c) -> {
            c.ack();
            got.countDown();
        });

        BatchPublishResult result = b.publishBatch(List.of(
                new BatchItem("ok", "one"),
                new BatchItem("missing-topic", "bad"),
                new BatchItem("ok", "two")));

        assertEquals(3, result.receipts().size());
        assertEquals(2, result.acceptedCount());
        assertEquals(1, result.rejectedCount());
        assertTrue(result.receipts().get(1).reason().startsWith("UNKNOWN_TOPIC"));
        assertTrue(got.await(2, TimeUnit.SECONDS));
    }

    @Test
    void failingSubscriberDoesNotAffectOtherSubscribersOrMessages() throws Exception {
        BrokerProperties fast = new BrokerProperties();
        fast.setAckTimeoutMillis(100);
        fast.setMaxAttempts(3);
        broker = new InMemoryMessageBroker(fast, new JsonMessageCodec());
        InMemoryMessageBroker b = broker;
        b.registerTopic("t", String.class, true);
        CountDownLatch goodGot = new CountDownLatch(2);
        b.subscribe("good", "t", (m, c) -> {
            c.ack();
            goodGot.countDown();
        });
        b.subscribe("bad", "t", (m, c) -> {
            throw new IllegalStateException("boom");
        });

        b.publish("t", "a");
        b.publish("t", "b");
        assertTrue(goodGot.await(2, TimeUnit.SECONDS));
        // bad 订阅者持续重试（不影响 good），最终耗尽后 DEAD。
        BrokerSnapshot snapshot = await(() -> {
            BrokerSnapshot s = b.snapshot();
            return s.stats().getDeadByReason().getOrDefault(DeadReason.RETRIES_EXHAUSTED, 0L) >= 2;
        }, 10_000);
        assertNotNull(snapshot);
        assertEquals(2, b.snapshot().stats().getDeliveriesAcked());
    }

    private BrokerSnapshot await(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            BrokerSnapshot snapshot = broker.snapshot();
            if (condition.getAsBoolean()) {
                return snapshot;
            }
            Thread.sleep(20);
        }
        fail("condition not met within timeout");
        return null;
    }
}
