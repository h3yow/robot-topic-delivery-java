package com.github.h3yow.robottopicdeliveryjava.config;

import com.github.h3yow.robottopicdeliveryjava.broker.MessageBroker;
import com.github.h3yow.robottopicdeliveryjava.codec.MessageCodec;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 将 broker 装配进 Spring 上下文；broker 随上下文关闭而关闭。
 */
@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
public class BrokerBeanConfig {

    @Bean
    public MessageCodec messageCodec() {
        return new MessageCodec();
    }

    @Bean(destroyMethod = "close")
    public MessageBroker messageBroker(BrokerProperties properties, MessageCodec codec) {
        return new MessageBroker(properties.toConfig(), codec);
    }
}
