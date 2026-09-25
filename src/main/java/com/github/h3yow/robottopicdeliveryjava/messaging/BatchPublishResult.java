package com.github.h3yow.robottopicdeliveryjava.messaging;

import java.util.List;

/**
 * 批量发布结果：逐条返回，任何一条被拒绝（未知主题/非法负载/编码失败/队列拒绝）
 * 都不影响同批其他消息。
 */
public record BatchPublishResult(List<PublishReceipt> receipts) {

    public int acceptedCount() {
        int count = 0;
        for (PublishReceipt receipt : receipts) {
            if (receipt.accepted()) {
                count++;
            }
        }
        return count;
    }

    public int rejectedCount() {
        int count = 0;
        for (PublishReceipt receipt : receipts) {
            if (!receipt.accepted()) {
                count++;
            }
        }
        return count;
    }
}
