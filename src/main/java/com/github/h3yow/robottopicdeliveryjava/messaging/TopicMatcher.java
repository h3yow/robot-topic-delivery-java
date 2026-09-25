package com.github.h3yow.robottopicdeliveryjava.messaging;

import com.github.h3yow.robottopicdeliveryjava.messaging.exception.InvalidTopicException;

/**
 * 层级主题匹配：层级以 {@code /} 分隔；{@code +} 匹配恰好一个层级；
 * {@code #} 仅可作为最后一个层级，匹配零个或多个层级。
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    /**
     * 校验具体主题（发布/注册用）：非空、层级以 {@code /} 分隔且均非空，
     * 不允许任何通配符。
     */
    public static String validateTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new InvalidTopicException("topic must not be null or empty");
        }
        String[] levels = topic.split("/", -1);
        for (String level : levels) {
            if (level.isEmpty()) {
                throw new InvalidTopicException(
                        "topic '" + topic + "' contains an empty level");
            }
            if (level.contains("+") || level.contains("#")) {
                throw new InvalidTopicException(
                        "topic '" + topic + "' must not contain wildcards '+' or '#'");
            }
        }
        return topic;
    }

    /**
     * 校验订阅过滤规则：{@code +} 独占一个层级，{@code #} 只能是最后一个层级。
     */
    public static String validateFilter(String filter) {
        if (filter == null || filter.isEmpty()) {
            throw new InvalidTopicException("filter must not be null or empty");
        }
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (level.isEmpty()) {
                throw new InvalidTopicException(
                        "filter '" + filter + "' contains an empty level");
            }
            if (level.equals("#")) {
                if (i != levels.length - 1) {
                    throw new InvalidTopicException(
                            "filter '" + filter + "': '#' must be the last level");
                }
                continue;
            }
            if (level.contains("#")) {
                throw new InvalidTopicException(
                        "filter '" + filter + "': '#' must occupy a whole level");
            }
            if (level.contains("+") && !level.equals("+")) {
                throw new InvalidTopicException(
                        "filter '" + filter + "': '+' must occupy a whole level");
            }
        }
        return filter;
    }

    /**
     * 判断过滤规则是否能匹配具体主题。
     * 规则：{@code +} 匹配恰好一个层级；结尾 {@code #} 匹配零个或多个层级；
     * 其余层级按字面量精确匹配。
     */
    public static boolean matches(String filter, String topic) {
        String[] fs = filter.split("/", -1);
        String[] ts = topic.split("/", -1);
        int fi = 0;
        int ti = 0;
        while (fi < fs.length) {
            String fl = fs[fi];
            if ("#".equals(fl)) {
                // 结尾多层通配，吞掉主题剩余全部层级（含零层）。
                return true;
            }
            if (ti >= ts.length) {
                // 过滤规则仍有层级而主题已耗尽（且未遇到 #）。
                return false;
            }
            if ("+".equals(fl)) {
                fi++;
                ti++;
                continue;
            }
            if (!fl.equals(ts[ti])) {
                return false;
            }
            fi++;
            ti++;
        }
        return ti == ts.length;
    }
}
