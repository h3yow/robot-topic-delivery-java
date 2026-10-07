package com.github.h3yow.robottopicdeliveryjava.broker;

import com.github.h3yow.robottopicdeliveryjava.topic.Topic;

import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 死信留存存储：进程内、有界、按终结时间有序。
 *
 * 取舍与记账（所有方法线程安全）：
 * - 容量满且策略为 EVICT_OLDEST：新记录留存，淘汰最旧的一条（无论其 RETAINED/REDRIVEN），
 *   淘汰数返回给调用方记账 deadEvicted；
 * - 容量满且策略为 REJECT_NEW：新记录不留存，archive 返回 null，调用方记账 deadArchiveRejected。
 * - REDRIVEN 记录仍继续留存（占用容量），因此
 *   archived == retainedNow + redrivenNow + evicted 的记账关系恒成立，可随时核对。
 *
 * 记录以 (subscriptionId, 原始 messageId) 唯一定位；同一 (sub,msg) 不重复归档。
 */
public class DeadLetterStore {

    private final int capacity;
    private final DeadLetterOverflowPolicy policy;

    private final Map<String, DeadMessage> byRecordId = new HashMap<>();
    private final Map<Key, String> index = new HashMap<>();
    private final Deque<String> order = new ArrayDeque<>(); // recordId，队首最旧

    public DeadLetterStore(int capacity, DeadLetterOverflowPolicy policy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("dead letter capacity must be positive");
        }
        this.capacity = capacity;
        this.policy = policy == null ? DeadLetterOverflowPolicy.EVICT_OLDEST : policy;
    }

    private record Key(String subscriptionId, String messageId) {
    }

    /**
     * 归档一条终止消息。返回留存记录；REJECT_NEW 且容量满时返回 null。
     *
     * @param evictedOut 长度至少 1 的数组，用于回传本次淘汰的记录数（EVICT_OLDEST 时为 1，否则 0）
     */
    public synchronized DeadMessage archive(String messageId, String originMessageId, String subscriptionId,
                                            Topic topic, byte[] payload, int attempts,
                                            Instant terminatedAt, int[] evictedOut) {
        Key key = new Key(subscriptionId, messageId);
        if (index.containsKey(key)) {
            return byRecordId.get(index.get(key)); // 幂等：不重复归档
        }
        if (byRecordId.size() >= capacity) {
            if (policy == DeadLetterOverflowPolicy.REJECT_NEW) {
                return null;
            }
            String oldestId = order.pollFirst();
            if (oldestId != null) {
                DeadMessage removed = byRecordId.remove(oldestId);
                if (removed != null) {
                    index.remove(new Key(removed.subscriptionId(), removed.messageId()));
                }
                if (evictedOut != null && evictedOut.length > 0) {
                    evictedOut[0]++;
                }
            }
        }
        DeadMessage rec = new DeadMessage(
                UUID.randomUUID().toString(), messageId, originMessageId, subscriptionId,
                topic, payload, attempts, terminatedAt, DeadMessageStatus.RETAINED, null);
        byRecordId.put(rec.recordId(), rec);
        index.put(key, rec.recordId());
        order.addLast(rec.recordId());
        return rec;
    }

    /** 兼容：不关心淘汰回传的归档入口。 */
    public DeadMessage archive(String messageId, String originMessageId, String subscriptionId,
                               Topic topic, byte[] payload, int attempts, Instant terminatedAt) {
        return archive(messageId, originMessageId, subscriptionId, topic, payload, attempts,
                terminatedAt, null);
    }

    public synchronized DeadMessage find(String subscriptionId, String messageId) {
        String rid = index.get(new Key(subscriptionId, messageId));
        return rid == null ? null : byRecordId.get(rid);
    }

    public synchronized DeadMessage get(String recordId) {
        return byRecordId.get(recordId);
    }

    /** 按终结时间从旧到新列出全部留存记录（含已重投的，以 REDRIVEN 状态标注）。 */
    public synchronized List<DeadMessage> list() {
        List<DeadMessage> out = new ArrayList<>(order.size());
        for (String rid : order) {
            DeadMessage r = byRecordId.get(rid);
            if (r != null) {
                out.add(r);
            }
        }
        return out;
    }

    public synchronized List<DeadMessage> listForSubscription(String subscriptionId) {
        return list().stream().filter(r -> r.subscriptionId().equals(subscriptionId)).toList();
    }

    public synchronized int size() {
        return byRecordId.size();
    }

    public synchronized int retainedCount() {
        int n = 0;
        for (DeadMessage r : byRecordId.values()) {
            if (r.status() == DeadMessageStatus.RETAINED) {
                n++;
            }
        }
        return n;
    }

    public synchronized int redrivenCount() {
        return byRecordId.size() - retainedCount();
    }

    public synchronized int retainedCountForSubscription(String subscriptionId) {
        return (int) byRecordId.values().stream()
                .filter(r -> r.subscriptionId().equals(subscriptionId)
                        && r.status() == DeadMessageStatus.RETAINED)
                .count();
    }

    public synchronized int redrivenCountForSubscription(String subscriptionId) {
        return (int) byRecordId.values().stream()
                .filter(r -> r.subscriptionId().equals(subscriptionId)
                        && r.status() == DeadMessageStatus.REDRIVEN)
                .count();
    }

    /**
     * 将一条记录原子地标记为已重投；仅当当前状态为 RETAINED 时成功。
     * 返回更新后的记录；若已被重投或已被淘汰则返回 null（调用方据此区分挡下原因）。
     */
    public synchronized DeadMessage markRedelivered(DeadMessage record, String newMessageId) {
        DeadMessage current = byRecordId.get(record.recordId());
        if (current == null || current.status() != DeadMessageStatus.RETAINED) {
            return null;
        }
        DeadMessage updated = new DeadMessage(
                current.recordId(), current.messageId(), current.originMessageId(),
                current.subscriptionId(), current.topic(), current.payload(), current.attempts(),
                current.terminatedAt(), DeadMessageStatus.REDRIVEN, newMessageId);
        byRecordId.put(current.recordId(), updated);
        return updated;
    }
}
