package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.TopicMatcher;
import com.github.h3yow.robottopicdeliveryjava.messaging.exception.InvalidTopicException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TopicMatcherTest {

    @Test
    void exactMatch() {
        assertTrue(TopicMatcher.matches("robot/r1/status", "robot/r1/status"));
        assertFalse(TopicMatcher.matches("robot/r1/status", "robot/r1/status/battery"));
        assertFalse(TopicMatcher.matches("robot/r1/status", "robot/r2/status"));
    }

    @Test
    void singleLevelWildcardPlus() {
        assertTrue(TopicMatcher.matches("robot/+/status", "robot/r1/status"));
        assertFalse(TopicMatcher.matches("robot/+/status", "robot/r1/telemetry"));
        assertFalse(TopicMatcher.matches("robot/+/status", "robot/a/b/status"));
        assertTrue(TopicMatcher.matches("+/+/+", "a/b/c"));
    }

    @Test
    void multiLevelWildcardHash() {
        assertTrue(TopicMatcher.matches("robot/#", "robot/r1/status/battery"));
        assertTrue(TopicMatcher.matches("robot/#", "robot"));
        assertFalse(TopicMatcher.matches("robot/#", "factory/robot/x"));
        assertTrue(TopicMatcher.matches("#", "anything/at/all"));
        assertTrue(TopicMatcher.matches("robot/+/status/#", "robot/r1/status/a/b"));
        assertTrue(TopicMatcher.matches("robot/+/status/#", "robot/r1/status"));
    }

    @Test
    void invalidConcreteTopicsAreRejectedWithReason() {
        InvalidTopicException e1 = assertThrows(InvalidTopicException.class,
                () -> TopicMatcher.validateTopic(""));
        assertTrue(e1.getReason().contains("empty"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateTopic("a//b"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateTopic("a/+/b"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateTopic("a/#"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateTopic("/a"));
    }

    @Test
    void invalidFiltersAreRejectedWithReason() {
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateFilter("a/#/b"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateFilter("a/+b"));
        assertThrows(InvalidTopicException.class, () -> TopicMatcher.validateFilter("a//b"));
        assertDoesNotThrow(() -> TopicMatcher.validateFilter("#"));
        assertDoesNotThrow(() -> TopicMatcher.validateFilter("a/+/b/#"));
    }
}
