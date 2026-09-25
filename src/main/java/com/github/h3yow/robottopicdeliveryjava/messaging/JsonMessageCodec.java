package com.github.h3yow.robottopicdeliveryjava.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.robottopicdeliveryjava.messaging.exception.EncodingFailureException;

/** 基于 Jackson 的 JSON 编解码实现，所有底层失败归一为可区分的 {@link EncodingFailureException}。 */
public class JsonMessageCodec implements MessageCodec {

    private final ObjectMapper objectMapper;

    public JsonMessageCodec() {
        this(new ObjectMapper());
    }

    public JsonMessageCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] encode(Object payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new EncodingFailureException(
                    "JSON encode failed for payload type "
                            + (payload == null ? "null" : payload.getClass().getName())
                            + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Object decode(byte[] data, Class<?> payloadType) {
        if (data == null) {
            throw new EncodingFailureException("decode failed: encoded data is null", null);
        }
        try {
            return objectMapper.readValue(data, payloadType);
        } catch (Exception e) {
            throw new EncodingFailureException(
                    "JSON decode failed for declared type "
                            + (payloadType == null ? "null" : payloadType.getName())
                            + ": " + e.getMessage(), e);
        }
    }
}
