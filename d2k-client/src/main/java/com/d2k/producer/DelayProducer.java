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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayProducer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DelayProducer.class);
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final Producer<K, V> producer;

    /**
     * 使用 Properties 创建 DelayProducer
     *
     * @param props Kafka 生产者配置
     */
    public DelayProducer(Map<String, Object> props) {
        log.info("Initializing DelayProducer with properties: {}", props);
        this.producer = buildProducer(props);
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
     * 发送延迟消息到指定分区
     *
     * @param topic     主题
     * @param partition 分区号
     * @param key       消息键
     * @param value     消息值
     * @param delayMs   延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelayToPartition(String topic, Integer partition, K key, V value, long delayMs) {
        long actualDelayMs = Math.max(0L, delayMs);
        log.debug("Sending message to topic {} partition {} with key {} and d2k-delay-ms={}", topic, partition, key, actualDelayMs);
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, value);
        record.headers().add(new RecordHeader("d2k-delay-ms",
                Long.toString(actualDelayMs).getBytes(StandardCharsets.UTF_8)));
        return producer.send(record);
    }

    /**
     * 发送定时消息到指定分区（指定投递时间戳）
     *
     * @param topic            主题
     * @param partition        分区号
     * @param key              消息键
     * @param value            消息值
     * @param deliverAtEpochMs 投递时间戳（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendDeliverAtToPartition(String topic, Integer partition, K key, V value, long deliverAtEpochMs) {
        log.debug("Sending message to topic {} partition {} with key {} and d2k-deliver-at={}", topic, partition, key, deliverAtEpochMs);
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, value);
        record.headers().add(new RecordHeader("d2k-deliver-at",
                Long.toString(deliverAtEpochMs).getBytes(StandardCharsets.UTF_8)));
        return producer.send(record);
    }

    /**
     * 获取底层的Producer实例
     * 
     * @return Producer实例
     */
    public Producer<K, V> getProducer() {
        return producer;
    }
    
    public void close() {
        log.info("Closing DelayProducer");
        producer.flush();
        producer.close();
    }
}