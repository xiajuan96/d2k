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
import org.apache.kafka.common.serialization.StringSerializer;
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

    private static final String WITH_DELAY_HEADER = "d2k-delay-ms";
    private static final String DELIVER_AT_HEADER = "d2k-deliver-at";

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
            copy.put(ProducerConfig.CLIENT_ID_CONFIG, "default-delay-sender-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
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

    public static DelayProducer<String, String> buildStringProducer(String bootstrapServers) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "default-string-delay-producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        return new DelayProducer<>(producerProps);
    }

    /**
     * 发送延迟消息（指定延迟时间）
     *
     * @param topic   主题
     * @param value   消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, V value, long delayMs) {
        return sendWithDelay(topic, null, value, delayMs, null);
    }

    /**
     * 异步发送延迟消息（指定延迟时间）
     *
     * @param topic    主题
     * @param value    消息值
     * @param delayMs  延迟时间（毫秒）
     * @param callback 回调接口
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, V value, long delayMs, DelayCallback callback) {
        return sendWithDelay(topic, null, value, delayMs, callback);
    }

    /**
     * 异步发送延迟消息（指定延迟时间）
     *
     * @param topic   主题
     * @param key     消息键
     * @param value   消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, K key, V value, long delayMs) {
        return sendWithDelay(topic, null, key, value, delayMs, null);
    }


    /**
     * 异步发送延迟消息（指定延迟时间）
     *
     * @param topic    主题
     * @param key      消息键
     * @param value    消息值
     * @param delayMs  延迟时间（毫秒）
     * @param callback 回调接口
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, K key, V value, long delayMs, DelayCallback callback) {
        return sendWithDelay(topic, null, key, value, delayMs, callback);
    }

    /**
     * 异步发送延迟消息到指定分区
     *
     * @param topic     主题
     * @param partition 分区号
     * @param key       消息键
     * @param value     消息值
     * @param delayMs   延迟时间（毫秒）
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic, Integer partition, K key, V value, long delayMs) {
        return sendWithDelay(topic, partition, key, value, delayMs, null);
    }


    /**
     * 发送延迟消息（指定延迟时间）
     *
     * @param topic     主题
     * @param partition 分区号
     * @param key       消息键
     * @param value     消息值
     * @param delayMs   延迟时间（毫秒）
     * @param callback  回调接口
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendWithDelay(String topic,
                                                Integer partition,
                                                K key,
                                                V value,
                                                long delayMs,
                                                DelayCallback callback) {
        long actualDelayMs = Math.max(0L, delayMs);
        log.debug("Sending message to topic {} with key {} and delayMs={}", topic, key, actualDelayMs);
        ProducerRecord<K, V> record;
        record = new ProducerRecord<>(topic, partition, System.currentTimeMillis(), key, value);
        record.headers()
                .add(new RecordHeader(WITH_DELAY_HEADER, Long.toString(actualDelayMs).getBytes(StandardCharsets.UTF_8)));

        return doSend(record, callback);
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
        return sendDeliverAt(topic, null, key, value, deliverAtEpochMs, null);
    }

    /**
     * 异步发送定时消息（指定投递时间戳）
     *
     * @param topic            主题
     * @param key              消息键
     * @param value            消息值
     * @param deliverAtEpochMs 投递时间戳（毫秒）
     * @param callback         回调接口
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendDeliverAt(String topic, K key, V value, long deliverAtEpochMs, DelayCallback callback) {
        return sendDeliverAt(topic, null, key, value, deliverAtEpochMs, callback);
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
    public Future<RecordMetadata> sendDeliverAt(String topic, Integer partition, K key, V value, long deliverAtEpochMs) {
        return sendDeliverAt(topic, partition, key, value, deliverAtEpochMs, null);
    }

    /**
     * 异步发送定时消息到指定分区（指定投递时间戳）
     *
     * @param topic            主题
     * @param partition        分区号
     * @param key              消息键
     * @param value            消息值
     * @param deliverAtEpochMs 投递时间戳（毫秒）
     * @param callback         回调接口
     * @return Future<RecordMetadata>
     */
    public Future<RecordMetadata> sendDeliverAt(String topic,
                                                Integer partition,
                                                K key,
                                                V value,
                                                long deliverAtEpochMs,
                                                DelayCallback callback) {
        log.debug("Sending message to topic {}, partition {} with key {} and deliverAt={}", topic, partition, key, deliverAtEpochMs);
        ProducerRecord<K, V> record;
        record = new ProducerRecord<>(topic, partition, System.currentTimeMillis(), key, value);
        record.headers().add(new RecordHeader(DELIVER_AT_HEADER,
                Long.toString(deliverAtEpochMs).getBytes(StandardCharsets.UTF_8)));

        return doSend(record, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, DelayCallback callback) {
        return producer.send(record, (metadata, exception) -> {
            try {
                if (exception != null) {
                    log.error("Failed to send message to topic {} partition {}",
                            metadata.topic(), metadata.partition(), exception);
                    if (callback != null) {
                        callback.onFailure(exception);
                    }
                } else {
                    log.debug("Successfully sent message  metadata: {}", metadata);
                    if (callback != null) {
                        callback.onSuccess(metadata);
                    }
                }
            } catch (Exception callbackException) {
                log.error("Exception in callback execution for message to topic {} partition {}",
                        metadata.topic(), metadata.partition(), callbackException);
            }
        });
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