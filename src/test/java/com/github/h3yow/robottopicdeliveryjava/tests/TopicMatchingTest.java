package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.topic.InvalidTopicException;
import com.github.h3yow.robottopicdeliveryjava.topic.Topic;
import com.github.h3yow.robottopicdeliveryjava.topic.TopicFilter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class TopicMatchingTest {

    private static final Logger log = LoggerFactory.getLogger(TopicMatchingTest.class);

    @Test
    void exactMatch() {
        TopicFilter f = TopicFilter.of("robot/arm/position");
        assertTrue(f.matches(Topic.of("robot/arm/position")));
        assertFalse(f.matches(Topic.of("robot/arm/velocity")));
        assertFalse(f.matches(Topic.of("robot/arm")));
        assertFalse(f.matches(Topic.of("robot/arm/position/x")));
        log.info("[case=exactMatch] filter={} 判定依据=逐层全等", f.pattern());
    }

    @Test
    void singleLevelWildcard() {
        TopicFilter f = TopicFilter.of("robot/+/position");
        assertTrue(f.matches(Topic.of("robot/arm/position")));
        assertTrue(f.matches(Topic.of("robot/leg/position")));
        assertFalse(f.matches(Topic.of("robot/position")));
        assertFalse(f.matches(Topic.of("robot/arm/joint/position")));
        log.info("[case=singleLevelWildcard] filter={} 判定依据='+'占且仅占一层", f.pattern());
    }

    @Test
    void multiLevelWildcard() {
        TopicFilter f = TopicFilter.of("robot/#");
        assertTrue(f.matches(Topic.of("robot")));
        assertTrue(f.matches(Topic.of("robot/arm")));
        assertTrue(f.matches(Topic.of("robot/arm/joint/position")));
        assertFalse(f.matches(Topic.of("factory/robot")));
        log.info("[case=multiLevelWildcard] filter={} 判定依据='#'匹配剩余任意层(含0层)", f.pattern());
    }

    @Test
    void invalidTopicsRejected() {
        assertThrows(InvalidTopicException.class, () -> Topic.of(""));
        assertThrows(InvalidTopicException.class, () -> Topic.of("a//b"));
        assertThrows(InvalidTopicException.class, () -> Topic.of("a/+/b"));
        assertThrows(InvalidTopicException.class, () -> Topic.of("a/#"));
        assertThrows(InvalidTopicException.class, () -> Topic.of("a/ b/c"));
        log.info("[case=invalidTopicsRejected] 判定依据=空层/通配符/空白字符均拒绝");
    }

    @Test
    void invalidFiltersRejected() {
        assertThrows(InvalidTopicException.class, () -> TopicFilter.of("a/#/b"));
        assertThrows(InvalidTopicException.class, () -> TopicFilter.of(""));
        assertThrows(InvalidTopicException.class, () -> TopicFilter.of("a//+"));
        log.info("[case=invalidFiltersRejected] 判定依据='#'必须在最后一层且不允许空层");
    }
}
