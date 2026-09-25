package com.github.h3yow.robottopicdeliveryjava.topic;

import java.util.List;

/**
 * 订阅过滤器，支持精确匹配与通配：
 *   '+' 匹配单层，'#' 匹配剩余任意层（必须位于最后一层）。
 * 例如 "robot/+/position" 匹配 "robot/arm/position"，"robot/#" 匹配 "robot" 及其全部子层级。
 */
public final class TopicFilter {

    private final String pattern;
    private final List<String> levels;

    private TopicFilter(String pattern, List<String> levels) {
        this.pattern = pattern;
        this.levels = levels;
    }

    public static TopicFilter of(String pattern) {
        if (pattern == null || pattern.isBlank()) {
            throw new InvalidTopicException("topic filter must not be null or blank");
        }
        String[] parts = pattern.split("/", -1);
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            if (p.isEmpty()) {
                throw new InvalidTopicException("filter level must not be empty: '" + pattern + "'");
            }
            if (p.equals("#")) {
                if (i != parts.length - 1) {
                    throw new InvalidTopicException("'#' must be the last level: '" + pattern + "'");
                }
            } else if (p.equals("+")) {
                // ok
            } else {
                // 复用 Topic 的单层校验，禁止通配符混入普通层与空白字符
                Topic.of(p);
            }
        }
        return new TopicFilter(pattern, List.of(parts));
    }

    public boolean matches(Topic topic) {
        List<String> t = topic.levels();
        for (int i = 0; i < levels.size(); i++) {
            String f = levels.get(i);
            if (f.equals("#")) {
                return true;
            }
            if (i >= t.size()) {
                return false;
            }
            if (f.equals("+")) {
                continue;
            }
            if (!f.equals(t.get(i))) {
                return false;
            }
        }
        return levels.size() == t.size();
    }

    public String pattern() {
        return pattern;
    }

    @Override
    public String toString() {
        return pattern;
    }
}
