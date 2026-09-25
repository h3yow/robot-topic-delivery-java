package com.github.h3yow.robottopicdeliveryjava.messaging;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 消息能力的 Spring 装配：仅依赖容器内 Jackson，无任何外部服务。 */
@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
public class MessagingConfig {

    @Bean
    public MessageCodec messageCodec() {
        return new JsonMessageCodec();
    }

    @Bean(destroyMethod = "close")
    public MessageBroker messageBroker(BrokerProperties properties, MessageCodec messageCodec) {
        return new InMemoryMessageBroker(properties, messageCodec);
    }
}
