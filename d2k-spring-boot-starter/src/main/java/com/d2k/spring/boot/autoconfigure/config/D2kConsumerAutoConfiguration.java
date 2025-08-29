package com.d2k.spring.boot.autoconfigure.config;


import com.d2k.spring.boot.autoconfigure.D2kProperties;
import com.d2k.spring.boot.autoconfigure.annotation.D2kListenerAnnotationBeanPostProcessor;
import com.d2k.spring.boot.autoconfigure.factory.DelayConsumerContainerFactory;
import com.d2k.spring.boot.autoconfigure.manager.D2kConsumerManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * D2K Consumer 自动配置类
 * 专门负责 DelayConsumerContainer 和相关 Bean 的配置
 */
@Configuration
@ConditionalOnClass(DelayConsumerContainerFactory.class)
@EnableConfigurationProperties(D2kProperties.class)
public class D2kConsumerAutoConfiguration {

    /**
     * 配置 DelayConsumerContainerFactory Bean
     */
    @Bean
    @ConditionalOnMissingBean
    public DelayConsumerContainerFactory delayConsumerContainerFactory(D2kProperties properties) {
        return new DelayConsumerContainerFactory(properties);
    }

    /**
     * 配置 D2kConsumerManager Bean
     */
    @Bean
    @ConditionalOnMissingBean
    public D2kConsumerManager d2kConsumerManager() {
        return new D2kConsumerManager();
    }

    /**
     * 配置 D2kListenerAnnotationBeanPostProcessor Bean
     */
    @Bean
    @ConditionalOnMissingBean
    public D2kListenerAnnotationBeanPostProcessor d2kListenerAnnotationBeanPostProcessor() {
        return new D2kListenerAnnotationBeanPostProcessor();
    }
}