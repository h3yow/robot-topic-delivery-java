package com.github.h3yow.robottopicdeliveryjava.topic;

import java.util.List;

/**
 * 层级主题名，形如 "robot/arm/position"。
 * 规则：以 '/' 分层；每层非空；不允许空白字符；不允许通配符 '+'、'#'。
 */
public final class Topic {

    private final String name;
    private final List<String> levels;

    private Topic(String name, List<String> levels) {
        this.name = name;
        this.levels = levels;
    }

    public static Topic of(String name) {
        if (name == null || name.isBlank()) {
            throw new InvalidTopicException("topic name must not be null or blank");
        }
        String[] parts = name.split("/", -1);
        for (String p : parts) {
            if (p.isEmpty()) {
                throw new InvalidTopicException("topic level must not be empty: '" + name + "'");
            }
            if (p.indexOf('+') >= 0 || p.indexOf('#') >= 0) {
                throw new InvalidTopicException("topic name must not contain wildcards: '" + name + "'");
            }
            for (char c : p.toCharArray()) {
                if (Character.isWhitespace(c)) {
                    throw new InvalidTopicException("topic level must not contain whitespace: '" + name + "'");
                }
            }
        }
        return new Topic(name, List.of(parts));
    }

    public String name() {
        return name;
    }

    public List<String> levels() {
        return levels;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Topic t && t.name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
