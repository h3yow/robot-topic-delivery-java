package com.github.h3yow.robottopicdeliveryjava.messaging;

import com.github.h3yow.robottopicdeliveryjava.messaging.exception.EncodingFailureException;

/** 消息负载编解码抽象，默认实现为 JSON。 */
public interface MessageCodec {

    /** 将负载对象编码为字节。失败时抛 {@link EncodingFailureException}。 */
    byte[] encode(Object payload);

    /** 按声明类型解码字节。失败时抛 {@link EncodingFailureException}。 */
    Object decode(byte[] data, Class<?> payloadType);
}
