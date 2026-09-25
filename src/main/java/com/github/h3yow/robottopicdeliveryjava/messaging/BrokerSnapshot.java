package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.List;

/** Broker 某一时刻的完整诊断视图。 */
public record BrokerSnapshot(
        long epochNanos,
        BrokerStats stats,
        List<SubscriptionSnapshot> subscriptions
) {
}
