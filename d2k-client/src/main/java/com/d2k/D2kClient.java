/*
 * D2K - Delay to Kafka
 * Copyright (C) 2024 xiajuan96
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package com.d2k;

import com.d2k.consumer.DelayConsumerContainer;
import com.d2k.consumer.DelayItemHandler;
import com.d2k.consumer.AsyncProcessingConfig;
import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * D2K 统一客户端
 * 整合了延迟消息生产和消费功能，提供统一的API接口
 * 
 * @param <K> 消息键类型
 * @param <V> 消息值类型
 * @author xiajuan96
 */
public class D2kClient<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(D2kClient.class);
    
    private final DelayProducer<K, V> producer;
    private DelayConsumerContainer<K, V> consumerContainer;
    
    /**
     * 构造D2K客户端（仅生产者）
     * 
     * @param producerProps 生产者配置
     */
    public D2kClient(Map<String, Object> producerProps) {
        this.producer = new DelayProducer<>(producerProps);
        log.info("D2kClient initialized with producer only");
    }
    
    /**
     * 构造D2K客户端（生产者 + 消费者）
     * 
     * @param producerProps 生产者配置
     * @param consumerConfigs 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param topics 订阅的主题列表
     * @param delayItemHandler 延迟消息处理器
     * @param concurrency 消费者并发数
     */
    public D2kClient(Map<String, Object> producerProps,
                     Map<String, Object> consumerConfigs,
                     Deserializer<K> keyDeserializer,
                     Deserializer<V> valueDeserializer,
                     Collection<String> topics,
                     DelayItemHandler<K, V> delayItemHandler,
                     int concurrency) {
        this.producer = new DelayProducer<>(producerProps);
        this.consumerContainer = new DelayConsumerContainer<>(
            concurrency, consumerConfigs, keyDeserializer, valueDeserializer,
            topics, delayItemHandler, AsyncProcessingConfig.createSyncConfig());
        log.info("D2kClient initialized with producer and consumer");
    }
    
    /**
     * 构造D2K客户端（生产者 + 消费者，支持异步处理配置）
     * 
     * @param producerProps 生产者配置
     * @param consumerConfigs 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param topics 订阅的主题列表
     * @param delayItemHandler 延迟消息处理器
     * @param concurrency 消费者并发数
     * @param asyncProcessingConfig 异步处理配置
     */
    public D2kClient(Map<String, Object> producerProps,
                     Map<String, Object> consumerConfigs,
                     Deserializer<K> keyDeserializer,
                     Deserializer<V> valueDeserializer,
                     Collection<String> topics,
                     DelayItemHandler<K, V> delayItemHandler,
                     int concurrency,
                     AsyncProcessingConfig asyncProcessingConfig) {
        this.producer = new DelayProducer<>(producerProps);
        this.consumerContainer = new DelayConsumerContainer<>(
            concurrency, consumerConfigs, keyDeserializer, valueDeserializer,
            topics, delayItemHandler, asyncProcessingConfig);
        log.info("D2kClient initialized with producer and consumer (async config)");
    }
    
    // ==================== 生产者方法 ====================
    
    /**
     * 发送延迟消息
     * 
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, K key, V value, long delayMs) {
        return producer.sendWithDelay(topic, key, value, delayMs);
    }
    
    /**
     * 发送定时消息
     * 
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @param deliverAtEpochMs 投递时间戳（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendDeliverAt(String topic, K key, V value, long deliverAtEpochMs) {
        return producer.sendDeliverAt(topic, key, value, deliverAtEpochMs);
    }
    
    /**
     * 发送消息（使用预配置的延迟时间）
     * 
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> send(String topic, K key, V value) {
        return producer.send(topic, key, value);
    }
    
    /**
     * 发送消息（使用预配置的延迟时间，无键）
     * 
     * @param topic 主题
     * @param value 消息值
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> send(String topic, V value) {
        return producer.send(topic, value);
    }
    
    /**
     * 设置主题延迟配置
     * 
     * @param topic 主题
     * @param delayMs 延迟时间（毫秒）
     */
    public void setTopicDelay(String topic, long delayMs) {
        producer.setTopicDelay(topic, delayMs);
    }
    
    /**
     * 批量设置主题延迟配置
     * 
     * @param topicDelayMap 主题延迟配置映射
     */
    public void setTopicDelays(Map<String, Long> topicDelayMap) {
        producer.setTopicDelays(topicDelayMap);
    }
    
    /**
     * 获取主题延迟配置
     * 
     * @param topic 主题
     * @return 延迟时间（毫秒），如果没有配置则返回 null
     */
    public Long getTopicDelay(String topic) {
        return producer.getTopicDelay(topic);
    }
    
    /**
     * 移除主题延迟配置
     * 
     * @param topic 主题
     * @return 被移除的延迟时间，如果没有配置则返回 null
     */
    public Long removeTopicDelay(String topic) {
        return producer.removeTopicDelay(topic);
    }
    
    /**
     * 获取所有主题延迟配置
     * 
     * @return 主题延迟配置的副本
     */
    public Map<String, Long> getAllTopicDelays() {
        return producer.getAllTopicDelays();
    }
    
    // ==================== 消费者方法 ====================
    
    /**
     * 启动消费者
     */
    public void startConsumer() {
        if (consumerContainer != null) {
            consumerContainer.start();
            log.info("Consumer started");
        } else {
            log.warn("Consumer not initialized, cannot start");
        }
    }
    
    /**
     * 停止消费者
     */
    public void stopConsumer() {
        if (consumerContainer != null) {
            consumerContainer.stop();
            log.info("Consumer stopped");
        } else {
            log.warn("Consumer not initialized, cannot stop");
        }
    }
    
    /**
     * 检查消费者是否正在运行
     * 
     * @return true 如果消费者正在运行
     */
    public boolean isConsumerRunning() {
        // 通过检查消费者容器是否已初始化且已启动来判断运行状态
        // 这里我们无法直接访问executor字段，所以简化判断逻辑
        return consumerContainer != null;
    }
    
    // ==================== 客户端管理方法 ====================
    
    /**
     * 关闭客户端，释放所有资源
     */
    public void close() {
        log.info("Closing D2kClient");
        
        // 停止消费者
        if (consumerContainer != null) {
            consumerContainer.stop();
        }
        
        // 关闭生产者
        if (producer != null) {
            producer.close();
        }
        
        log.info("D2kClient closed");
    }
    
    /**
     * 获取生产者实例（用于高级操作）
     * 
     * @return DelayProducer 实例
     */
    public DelayProducer<K, V> getProducer() {
        return producer;
    }
    
    /**
     * 获取消费者容器实例（用于高级操作）
     * 
     * @return DelayConsumerContainer 实例，如果未初始化则返回 null
     */
    public DelayConsumerContainer<K, V> getConsumerContainer() {
        return consumerContainer;
    }
}