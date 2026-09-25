package com.github.h3yow.robottopicdeliveryjava.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 消息编解码器：将负载序列化为 JSON 字节，并可反向解析校验。
 * 负载为 null 或无法序列化时抛出 {@link EncodeException}。
 */
public class MessageCodec {

    private final ObjectMapper mapper = new ObjectMapper();

    public byte[] encode(Object payload) {
        if (payload == null) {
            throw new EncodeException("payload must not be null", null);
        }
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new EncodeException("failed to encode payload of type "
                    + payload.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    public JsonNode decode(byte[] data) {
        try {
            return mapper.readTree(data);
        } catch (Exception e) {
            throw new EncodeException("failed to decode payload bytes: " + e.getMessage(), e);
        }
    }
}
