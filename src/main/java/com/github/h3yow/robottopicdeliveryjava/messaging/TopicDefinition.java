package com.github.h3yow.robottopicdeliveryjava.messaging;

/**
 * 已注册主题定义。
 *
 * @param name         具体主题名（无通配符）
 * @param payloadType  该主题允许的负载类型
 * @param requirePayload 是否禁止 null 负载
 */
public record TopicDefinition(String name, Class<?> payloadType, boolean requirePayload) {
}
