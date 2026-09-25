package com.github.h3yow.robottopicdeliveryjava.tests;

import com.github.h3yow.robottopicdeliveryjava.messaging.JsonMessageCodec;
import com.github.h3yow.robottopicdeliveryjava.messaging.exception.EncodingFailureException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonMessageCodecTest {

    public record SensorEvent(String name, int value) {
    }

    @Test
    void roundTripJson() {
        JsonMessageCodec codec = new JsonMessageCodec();
        byte[] bytes = codec.encode(new SensorEvent("battery", 42));
        Object back = codec.decode(bytes, SensorEvent.class);
        assertEquals(new SensorEvent("battery", 42), back);
    }

    @Test
    void nonSerializablePayloadFailsWithEncodingFailure() {
        JsonMessageCodec codec = new JsonMessageCodec();
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);
        EncodingFailureException e = assertThrows(EncodingFailureException.class,
                () -> codec.encode(cyclic));
        assertTrue(e.getReason().startsWith("JSON encode failed"), e.getReason());
    }

    @Test
    void malformedJsonFailsWithEncodingFailure() {
        JsonMessageCodec codec = new JsonMessageCodec();
        EncodingFailureException e = assertThrows(EncodingFailureException.class,
                () -> codec.decode("{nope".getBytes(), SensorEvent.class));
        assertTrue(e.getReason().startsWith("JSON decode failed"), e.getReason());
    }
}
