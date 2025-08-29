package com.d2k.spring.boot.autoconfigure.template;

import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * D2K 模板类
 * 提供简化的延迟消息发送 API，支持泛型类型
 * 
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class D2kTemplate<K, V> {

    private final DelayProducer<K, V> delayProducer;

    public D2kTemplate(DelayProducer<K, V> delayProducer) {
        this.delayProducer = delayProducer;
    }

    /**
     * 异步发送延迟消息（使用预配置的延迟时间）
     *
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @return Future<RecordMetadata> 异步结果
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public Future<RecordMetadata> sendAsync(String topic, K key, V value) {
        return delayProducer.send(topic, key, value);
    }

    /**
     * 同步发送延迟消息（使用预配置的延迟时间）
     *
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @return RecordMetadata 发送结果
     * @throws RuntimeException 如果发送失败
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public RecordMetadata sendSync(String topic, K key, V value) {
        try {
            return delayProducer.send(topic, key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to send message synchronously", e);
        }
    }

    /**
     * 同步发送延迟消息（使用预配置的延迟时间，带超时）
     *
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return RecordMetadata 发送结果
     * @throws RuntimeException 如果发送失败或超时
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public RecordMetadata sendSync(String topic, K key, V value, long timeout, TimeUnit unit) {
        try {
            return delayProducer.send(topic, key, value).get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to send message synchronously", e);
        }
    }

    /**
     * 发送延迟消息（使用预配置的延迟时间）- 兼容性方法
     *
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public void send(String topic, K key, V value) {
        sendAsync(topic, key, value);
    }







    /**
     * 关闭生产者
     */
    public void close() {
        delayProducer.close();
    }
}