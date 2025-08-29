package com.d2k.spring.boot.autoconfigure.config;

import com.d2k.producer.DelayProducer;
import com.d2k.spring.boot.autoconfigure.D2kProperties;
import com.d2k.spring.boot.autoconfigure.template.D2kTemplate;
import com.d2k.spring.boot.autoconfigure.template.StringD2kTemplate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * D2K Producer 自动配置类
 * 专门负责 DelayProducer 和相关 Bean 的配置
 */
@Configuration
@ConditionalOnClass(DelayProducer.class)
@EnableConfigurationProperties(D2kProperties.class)
public class D2kProducerAutoConfiguration {

    private final D2kProperties d2kProperties;

    public D2kProducerAutoConfiguration(D2kProperties d2kProperties) {
        this.d2kProperties = d2kProperties;
    }

    /**
     * 配置 D2kTemplate Bean（Object类型）
     */
    @Bean
    @ConditionalOnMissingBean(name = "d2kTemplate")
    public D2kTemplate<Object, Object> d2kTemplate(D2kProperties properties) {
        Map<String, Object> producerProps = getProperties(properties);
        // 使用 topic 延迟配置创建 DelayProducer
        Map<String, Long> topicDelays = properties.getProducer().getTopicDelays();
        DelayProducer<Object, Object> delayProducer;
        if (topicDelays != null && !topicDelays.isEmpty()) {
            delayProducer = new DelayProducer<>(producerProps, topicDelays);
        } else {
            delayProducer = new DelayProducer<>(producerProps);
        }
        return new D2kTemplate<>(delayProducer);
    }

    /**
     * 配置 StringD2kTemplate Bean
     */
    @Bean
    @ConditionalOnMissingBean(name = "stringD2kTemplate")
    public StringD2kTemplate stringD2kTemplate() {
        // 直接创建StringDelayProducer实例
        Map<String, Object> configMap = getProperties(d2kProperties);
        Map<String, Long> topicDelays = d2kProperties.getProducer().getTopicDelays();

        DelayProducer<String, String> stringDelayProducer;
        if (topicDelays != null && !topicDelays.isEmpty()) {
            stringDelayProducer = new DelayProducer<>(configMap, topicDelays);
        } else {
            stringDelayProducer = new DelayProducer<>(configMap);
        }

        return new StringD2kTemplate(stringDelayProducer);
    }


    private Map<String, Object> getProperties(D2kProperties properties) {
        Map<String, Object> producerProps = new HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProducer().getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducer().getKeySerializer());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducer().getValueSerializer());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getProducer().getClientId());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, properties.getProducer().getRetries());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getProducer().getBatchSize());
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getProducer().getBufferMemory());
        return producerProps;
    }


}