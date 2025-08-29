package com.d2k.spring.boot.autoconfigure.template;

import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 字符串类型的 D2K 模板类
 * 继承 D2kTemplate<String, String>，提供简化的字符串消息发送接口
 * 参考 Spring 的 StringRedisTemplate 设计模式
 * 
 * @author xiajuan96
 * @date 2025/8/26
 */
public class StringD2kTemplate extends D2kTemplate<String, String> {

    public StringD2kTemplate(DelayProducer<String, String> delayProducer) {
        super(delayProducer);
    }

    /**
     * 异步发送延迟消息（使用预配置的延迟时间，无键）
     *
     * @param topic 主题
     * @param value 消息内容
     * @return Future<RecordMetadata> 异步结果
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public Future<RecordMetadata> sendAsync(String topic, String value) {
        return super.sendAsync(topic, null, value);
    }

    /**
     * 同步发送延迟消息（使用预配置的延迟时间，无键）
     *
     * @param topic 主题
     * @param value 消息内容
     * @return RecordMetadata 发送结果
     * @throws RuntimeException 如果发送失败
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public RecordMetadata sendSync(String topic, String value) {
        return super.sendSync(topic, null, value);
    }

    /**
     * 同步发送延迟消息（使用预配置的延迟时间，无键，带超时）
     *
     * @param topic 主题
     * @param value 消息内容
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return RecordMetadata 发送结果
     * @throws RuntimeException 如果发送失败或超时
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public RecordMetadata sendSync(String topic, String value, long timeout, TimeUnit unit) {
        return super.sendSync(topic, null, value, timeout, unit);
    }

    /**
     * 发送延迟消息（使用预配置的延迟时间，无键）- 兼容性方法
     *
     * @param topic 主题
     * @param value 消息内容
     * @throws IllegalArgumentException 如果 topic 没有配置延迟时间
     */
    public void send(String topic, String value) {
        sendAsync(topic, value);
    }
}