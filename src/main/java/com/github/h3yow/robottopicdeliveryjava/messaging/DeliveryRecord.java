package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一条消息针对某个订阅者的投递状态记录（每个匹配订阅者各持一条）。
 *
 * <p>合法状态迁移：
 * <pre>
 *   PENDING --(dispatch)--> IN_FLIGHT --(ack)----> ACKED（终态）
 *      ^                      |  |
 *      |------(nack/timeout/recover)---+
 *                             |
 *                             +--(dead)--> DEAD（终态）
 * </pre>
 * 所有迁移均为 CAS，保证重复确认 / 乱序（迟到）确认不会造成状态倒退或重复投递。
 */
public final class DeliveryRecord {

    /** 一次确认调用的判定结果。 */
    public enum AckOutcome {
        /** 确认被接受，记录进入 ACKED。 */
        ACKED,
        /** 重复确认：记录此前已 ACKED，拒绝且计数。 */
        DUPLICATE,
        /** 迟到/乱序确认：记录已超时重投或已终止，拒绝且计数。 */
        LATE
    }

    private final String subscriberId;
    private final PublishedMessage message;
    private final int maxAttempts;

    private final AtomicReference<DeliveryState> state = new AtomicReference<>(DeliveryState.PENDING);
    private final AtomicInteger attempt = new AtomicInteger(0);
    private final AtomicReference<DeadReason> deadReason = new AtomicReference<>();
    private volatile long inflightAtNanos;
    /** 重试退避：该时间（System.nanoTime）之前 worker 不得再次投递。 */
    private volatile long notBeforeNanos;

    public DeliveryRecord(String subscriberId, PublishedMessage message, int maxAttempts) {
        this.subscriberId = Objects.requireNonNull(subscriberId, "subscriberId");
        this.message = Objects.requireNonNull(message, "message");
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive: " + maxAttempts);
        }
        this.maxAttempts = maxAttempts;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public PublishedMessage getMessage() {
        return message;
    }

    public String getMessageId() {
        return message.getMessageId();
    }

    public DeliveryState getState() {
        return state.get();
    }

    public int getAttempt() {
        return attempt.get();
    }

    public DeadReason getDeadReason() {
        return deadReason.get();
    }

    public long getInflightAtNanos() {
        return inflightAtNanos;
    }

    public long getNotBeforeNanos() {
        return notBeforeNanos;
    }

    public void setNotBeforeNanos(long notBeforeNanos) {
        this.notBeforeNanos = notBeforeNanos;
    }

    /**
     * 开始一次投递：仅 PENDING -> IN_FLIGHT 合法。
     *
     * <p>上限判定在 attempt 自增之前完成，保证 {@code maxAttempts=N} 时处理器恰好最多被调用 N 次：
     * attempt==N 表示此前已投递过 N 次，调用方据此直接终止而不再自增。
     *
     * @return 本次投递的尝试序号（从 1 开始）；记录不在 PENDING 或已达上限时返回 -1
     */
    public int beginDispatch(long nowNanos) {
        if (state.get() != DeliveryState.PENDING) {
            return -1;
        }
        if (attempt.get() >= maxAttempts) {
            return -1;
        }
        if (!state.compareAndSet(DeliveryState.PENDING, DeliveryState.IN_FLIGHT)) {
            return -1;
        }
        int next = attempt.incrementAndGet();
        this.inflightAtNanos = nowNanos;
        return next;
    }

    /**
     * 显式确认。
     *
     * @param tokenAttempt 调用方（当次投递控制句柄）持有的尝试序号，用于识别乱序/迟到确认
     */
    public AckOutcome ack(int tokenAttempt) {
        DeliveryState current = state.get();
        if (current == DeliveryState.ACKED) {
            return AckOutcome.DUPLICATE;
        }
        if (current == DeliveryState.DEAD) {
            return AckOutcome.LATE;
        }
        if (current == DeliveryState.PENDING) {
            // 在途投递已被超时扫描/恢复收回，确认晚到。
            return AckOutcome.LATE;
        }
        // IN_FLIGHT：必须与当次尝试序号一致，否则属于上一轮的迟到确认。
        if (attempt.get() != tokenAttempt) {
            return AckOutcome.LATE;
        }
        if (state.compareAndSet(DeliveryState.IN_FLIGHT, DeliveryState.ACKED)) {
            return AckOutcome.ACKED;
        }
        // CAS 失败说明并发收敛（终止或再次超时），按迟到处理。
        return AckOutcome.LATE;
    }

    /**
     * 否定确认：仅当前在途且 token 匹配时，IN_FLIGHT -> PENDING。
     *
     * @return true 表示本次 nack 被接受（调用方据此重入队首）
     */
    public boolean nack(int tokenAttempt) {
        return state.get() == DeliveryState.IN_FLIGHT
                && attempt.get() == tokenAttempt
                && state.compareAndSet(DeliveryState.IN_FLIGHT, DeliveryState.PENDING);
    }

    /**
     * 超时扫描回收：在途且发起时间早于 deadlineNanos 时，IN_FLIGHT -> PENDING。
     * 由单一扫描线程调用；返回 true 表示调用方负责把记录重入队首。
     */
    public boolean timeoutIfExpired(long deadlineNanos) {
        return state.get() == DeliveryState.IN_FLIGHT
                && inflightAtNanos < deadlineNanos
                && state.compareAndSet(DeliveryState.IN_FLIGHT, DeliveryState.PENDING);
    }

    /**
     * 恢复补投：任何在途记录无条件收回为 PENDING（IN_FLIGHT -> PENDING）。
     * 返回 true 表示调用方需要重入队首。
     */
    public boolean reclaimInflight() {
        return state.compareAndSet(DeliveryState.IN_FLIGHT, DeliveryState.PENDING);
    }

    /** 进入 DEAD 终态；ACKED 记录不可终止，返回 false。 */
    public boolean markDead(DeadReason reason) {
        Objects.requireNonNull(reason, "reason");
        DeliveryState current;
        do {
            current = state.get();
            if (current == DeliveryState.ACKED || current == DeliveryState.DEAD) {
                return false;
            }
        } while (!state.compareAndSet(current, DeliveryState.DEAD));
        deadReason.set(reason);
        return true;
    }
}
