package com.github.h3yow.robottopicdeliveryjava.broker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 失败留存（死信）存储：有界、线程安全。
 *
 * <p>负责任意时刻只保留至多 capacity 条记录，溢出按 {@link OverflowPolicy} 确定性取舍并记账：
 * <ul>
 *   <li>{@link OverflowPolicy#DROP_OLDEST}：淘汰终结时间最早（即最早插入）的记录，新记录一定保留；
 *       被淘汰的老记录计入 {@code deadLetterEvictedOldest}。</li>
 *   <li>{@link OverflowPolicy#DROP_NEWEST}：新记录被拒绝保留（老记录原样保留），
 *       计入 {@code deadLetterEvictedNewest}。</li>
 * </ul>
 *
 * <p>重投的状态机（RETAINED -> REPLAYING -> REPLAYED）在此原子推进：
 * 只有处于 RETAINED 的记录能被标记为 REPLAYING，因此同一条留存不可能被重投两次；
 * 重投产生的新投递通过新消息 id 与源记录关联，终结时再把源记录推进到 REPLAYED。
 * 已终结的旧记录永不复活，重投始终是一次全新投递。
 */
public class DeadLetterStore {

    private final int capacity;
    private final OverflowPolicy policy;
    private final BrokerStats stats;

    /** 插入顺序即终结时间从老到新；所有访问在 this 锁内进行 */
    private final LinkedHashMap<String, DeadLetter> byId = new LinkedHashMap<>();
    /** 重投新消息 id -> 源留存记录 id */
    private final Map<String, String> replayToSource = new LinkedHashMap<>();

    DeadLetterStore(int capacity, OverflowPolicy policy, BrokerStats stats) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("deadLetterCapacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.policy = policy;
        this.stats = stats;
    }

    /**
     * 保留一条新终结记录；容量超限时按策略取舍。
     *
     * @return true=已保留；false=按 DROP_NEWEST 被拒绝保留
     */
    boolean retain(DeadLetter letter) {
        boolean retained;
        boolean evictedOldest = false;
        synchronized (this) {
            if (byId.size() >= capacity && policy == OverflowPolicy.DROP_NEWEST) {
                retained = false;
            } else {
                byId.put(letter.id(), letter);
                if (byId.size() > capacity) {
                    // DROP_OLDEST：插入后超容量即淘汰最老，保证任意时刻 size <= capacity
                    Iterator<String> it = byId.keySet().iterator();
                    it.next();
                    it.remove();
                    evictedOldest = true;
                }
                retained = true;
            }
        }
        if (retained) {
            // retained 只统计“当前仍保留着”的新记录；准入后立即被挤出的老记录走 evictedOldest。
            if (!evictedOldest) {
                stats.incDeadLetterRetained();
            }
            if (evictedOldest) {
                // 新记录挤掉最老记录：老的那条算淘汰，新记录替代其留存名额（不重复计 retained）
                stats.incDeadLetterEvictedOldest();
            }
        } else {
            stats.incDeadLetterEvictedNewest();
        }
        return retained;
    }

    /**
     * 原子地把 RETAINED 记录推进到 REPLAYING 并登记新消息 id。
     *
     * @return 推进后的记录；记录不存在或已重投（非 RETAINED）时返回 null
     */
    synchronized DeadLetter markReplaying(String deadLetterId, String newMessageId) {
        DeadLetter letter = byId.get(deadLetterId);
        if (letter == null || letter.state() != DeadLetterState.RETAINED) {
            return null;
        }
        DeadLetter updated = new DeadLetter(letter.id(), letter.subscriptionId(), letter.topic(),
                letter.payload(), letter.attempts(), letter.diedAt(), DeadLetterState.REPLAYING,
                letter.originId(), newMessageId);
        byId.put(deadLetterId, updated);
        replayToSource.put(newMessageId, deadLetterId);
        return updated;
    }

    /** 重投已接受但入队失败（队列满）时回滚：恢复为 RETAINED，允许之后再次重投。 */
    synchronized DeadLetter rollbackReplaying(String newMessageId) {
        String sourceId = replayToSource.remove(newMessageId);
        if (sourceId == null) {
            return null;
        }
        DeadLetter letter = byId.get(sourceId);
        if (letter != null && letter.state() == DeadLetterState.REPLAYING) {
            DeadLetter restored = new DeadLetter(letter.id(), letter.subscriptionId(), letter.topic(),
                    letter.payload(), letter.attempts(), letter.diedAt(), DeadLetterState.RETAINED,
                    letter.originId(), null);
            byId.put(sourceId, restored);
            return restored;
        }
        return letter;
    }

    /** 重投产生的新投递终结（确认/再次终止/丢弃）后，把源记录推进到 REPLAYED。 */
    synchronized void markReplayed(String newMessageId) {
        String sourceId = replayToSource.remove(newMessageId);
        if (sourceId == null) {
            return;
        }
        DeadLetter letter = byId.get(sourceId);
        if (letter != null && letter.state() != DeadLetterState.REPLAYED) {
            byId.put(sourceId, new DeadLetter(letter.id(), letter.subscriptionId(), letter.topic(),
                    letter.payload(), letter.attempts(), letter.diedAt(), DeadLetterState.REPLAYED,
                    letter.originId(), letter.replayedAsMessageId()));
        }
    }

    synchronized DeadLetter get(String deadLetterId) {
        return byId.get(deadLetterId);
    }

    /** 当前留存的不可变快照（按终结时间从老到新）。 */
    synchronized List<DeadLetter> list() {
        return List.copyOf(new ArrayList<>(byId.values()));
    }

    synchronized int size() {
        return byId.size();
    }
}
