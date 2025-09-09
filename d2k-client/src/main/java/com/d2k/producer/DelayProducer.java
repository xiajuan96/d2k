/*
 * D2K - Delay to Kafka
 * Copyright (C) 2024 xiajuan96
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
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
package com.d2k.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayProducer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DelayProducer.class);
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final Producer<K, V> producer;
    private final Map<String, Long> topicDelayConfig;

    /**
     * 使用 Properties 创建 DelayProducer
     *
     * @param props Kafka 生产者配置
     */
    public DelayProducer(Map<String, Object> props) {
        log.info("Initializing DelayProducer with properties: {}", props);
        this.producer = buildProducer(props);
        this.topicDelayConfig = new ConcurrentHashMap<>();
    }


    /**
     * 使用 Properties 和 topic 延迟配置创建 DelayProducer
     *
     * @param props            Kafka 生产者配置
     * @param topicDelayConfig topic 和延迟时间的映射配置（毫秒）
     */
    public DelayProducer(Map<String, Object> props, Map<String, Long> topicDelayConfig) {
        log.info("Initializing DelayProducer with properties: {} and topic delay config: {}", props, topicDelayConfig);
        this.producer = buildProducer(props);
        this.topicDelayConfig = new ConcurrentHashMap<>(topicDelayConfig);
    }


    private KafkaProducer<K, V> buildProducer(Map<String, Object> props) {
        HashMap<String, Object> copy = new HashMap<>(props);
        Object clientId = copy.get(ProducerConfig.CLIENT_ID_CONFIG);
        if (clientId == null) {
            copy.put(ProducerConfig.CLIENT_ID_CONFIG, "");
        } else {
            copy.put(ProducerConfig.CLIENT_ID_CONFIG, clientId.toString() + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
        }
        return new KafkaProducer<>(copy);
    }

    /**
     * 使用现有 Producer 创建 DelayProducer
     *
     * @param producer Kafka 生产者实例
     */
    public DelayProducer(Producer<K, V> producer) {
        log.info("Initializing DelayProducer with provided producer");
        this.producer = producer;
        this.topicDelayConfig = new ConcurrentHashMap<>();
    }

    /**
     * 使用现有 Producer 和 topic 延迟配置创建 DelayProducer
     *
     * @param producer         Kafka 生产者实例
     * @param topicDelayConfig topic 和延迟时间的映射配置（毫秒）
     */
    public DelayProducer(Producer<K, V> producer, Map<String, Long> topicDelayConfig) {
        log.info("Initializing DelayProducer with provided producer and topic delay config: {}", topicDelayConfig);
        this.producer = producer;
        this.topicDelayConfig = new ConcurrentHashMap<>(topicDelayConfig);
    }

    /**
     * 发送延迟消息（使用预配置的延迟时间）
     *
     * @param topic 主题
     * @param key   消息键
     * @param value 消息值
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public Future<RecordMetadata> send(String topic, K key, V value) {
        Long delayMs = topicDelayConfig.get(topic);
        if (delayMs == null) {
            throw new IllegalArgumentException("Topic '" + topic + "' has no delay configuration. Please configure delay time for this topic or use sendWithDelay method.");
        }
        return sendWithDelay(topic, key, value, delayMs);
    }

    /**
     * 发送延迟消息（使用预配置的延迟时间，无键）
     *
     * @param topic 主题
     * @param value 消息值
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public Future<RecordMetadata> send(String topic, V value) {
        return send(topic, null, value);
    }

    /**
     * 发送延迟消息（指定延迟时间）
     *
     * @param topic   主题
     * @param key     消息键
     * @param value   消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, K key, V value, long delayMs) {
        long actualDelayMs = Math.max(0L, delayMs);
        log.debug("Sending message to topic {} with key {} and d2k-delay-ms={}", topic, key, actualDelayMs);
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        record.headers().add(new RecordHeader("d2k-delay-ms",
                Long.toString(actualDelayMs).getBytes(StandardCharsets.UTF_8)));
        return producer.send(record);
    }

    /**
     * 发送定时消息（指定投递时间戳）
     *
     * @param topic            主题
     * @param key              消息键
     * @param value            消息值
     * @param deliverAtEpochMs 投递时间戳（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendDeliverAt(String topic, K key, V value, long deliverAtEpochMs) {
        log.debug("Sending message to topic {} with key {} and d2k-deliver-at={}", topic, key, deliverAtEpochMs);
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        record.headers().add(new RecordHeader("d2k-deliver-at",
                Long.toString(deliverAtEpochMs).getBytes(StandardCharsets.UTF_8)));
        return producer.send(record);
    }

    /**
     * 添加或更新 topic 的延迟配置
     *
     * @param topic   主题
     * @param delayMs 延迟时间（毫秒）
     */
    public void setTopicDelay(String topic, long delayMs) {
        topicDelayConfig.put(topic, delayMs);
        log.info("Set delay configuration for topic '{}': {} ms", topic, delayMs);
    }

    /**
     * 批量设置 topic 延迟配置
     *
     * @param topicDelayMap topic 和延迟时间的映射
     */
    public void setTopicDelays(Map<String, Long> topicDelayMap) {
        topicDelayConfig.putAll(topicDelayMap);
        log.info("Set delay configurations: {}", topicDelayMap);
    }

    /**
     * 获取 topic 的延迟配置
     *
     * @param topic 主题
     * @return 延迟时间（毫秒），如果没有配置则返回 null
     */
    public Long getTopicDelay(String topic) {
        return topicDelayConfig.get(topic);
    }

    /**
     * 移除 topic 的延迟配置
     *
     * @param topic 主题
     * @return 被移除的延迟时间，如果没有配置则返回 null
     */
    public Long removeTopicDelay(String topic) {
        Long removed = topicDelayConfig.remove(topic);
        if (removed != null) {
            log.info("Removed delay configuration for topic '{}'", topic);
        }
        return removed;
    }

    /**
     * 获取所有 topic 延迟配置
     *
     * @return topic 延迟配置的副本
     */
    public Map<String, Long> getAllTopicDelays() {
        return new ConcurrentHashMap<>(topicDelayConfig);
    }

    public void close() {
        log.info("Closing DelayProducer");
        producer.flush();
        producer.close();
    }
}