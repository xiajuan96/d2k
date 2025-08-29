package com.d2k.test;

import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * DelayProducer 单元测试
 */
public class DelayProducerTest {

    private MockProducer<String, String> mockProducer;
    private DelayProducer<String, String> delayProducer;
    private Properties props;

    @Before
    public void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Test
    public void testCreateDelayProducerWithTopicConfig() {
        Map<String, Long> topicDelayConfig = new HashMap<>();
        topicDelayConfig.put("order-topic", 5000L);
        topicDelayConfig.put("payment-topic", 10000L);
        
        delayProducer = new DelayProducer<>(mockProducer, topicDelayConfig);
        
        assertEquals(Long.valueOf(5000L), delayProducer.getTopicDelay("order-topic"));
        assertEquals(Long.valueOf(10000L), delayProducer.getTopicDelay("payment-topic"));
        assertNull(delayProducer.getTopicDelay("unknown-topic"));
    }

    @Test
    public void testSendWithPreConfiguredDelay() throws Exception {
        Map<String, Long> topicDelayConfig = new HashMap<>();
        topicDelayConfig.put("test-topic", 3000L);
        
        delayProducer = new DelayProducer<>(mockProducer, topicDelayConfig);
        
        // 使用预配置延迟发送消息
        Future<RecordMetadata> future = delayProducer.send("test-topic", "key1", "value1");
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertEquals("key1", record.key());
        assertEquals("value1", record.value());
        
        // 验证延迟头部
        assertNotNull(record.headers().lastHeader("d2k-delay-ms"));
        String delayMs = new String(record.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        assertEquals("3000", delayMs);
    }

    @Test
    public void testSendWithoutKeyUsingPreConfiguredDelay() throws Exception {
        delayProducer = new DelayProducer<>(mockProducer);
        delayProducer.setTopicDelay("test-topic", 2000L);
        
        // 使用预配置延迟发送消息（无键）
        Future<RecordMetadata> future = delayProducer.send("test-topic", "value1");
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());
        assertEquals("value1", record.value());
        
        // 验证延迟头部
        String delayMs = new String(record.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        assertEquals("2000", delayMs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSendWithoutConfiguredTopicShouldThrowException() {
        delayProducer = new DelayProducer<>(mockProducer);
        
        // 尝试发送未配置延迟的topic，应该抛出异常
        delayProducer.send("unconfigured-topic", "key1", "value1");
    }

    @Test
    public void testSetAndGetTopicDelay() {
        delayProducer = new DelayProducer<>(mockProducer);
        
        // 设置延迟配置
        delayProducer.setTopicDelay("topic1", 1000L);
        delayProducer.setTopicDelay("topic2", 2000L);
        
        // 验证配置
        assertEquals(Long.valueOf(1000L), delayProducer.getTopicDelay("topic1"));
        assertEquals(Long.valueOf(2000L), delayProducer.getTopicDelay("topic2"));
        assertNull(delayProducer.getTopicDelay("topic3"));
    }

    @Test
    public void testSetTopicDelays() {
        delayProducer = new DelayProducer<>(mockProducer);
        
        Map<String, Long> delayMap = new HashMap<>();
        delayMap.put("topic1", 1000L);
        delayMap.put("topic2", 2000L);
        delayMap.put("topic3", 3000L);
        
        delayProducer.setTopicDelays(delayMap);
        
        assertEquals(Long.valueOf(1000L), delayProducer.getTopicDelay("topic1"));
        assertEquals(Long.valueOf(2000L), delayProducer.getTopicDelay("topic2"));
        assertEquals(Long.valueOf(3000L), delayProducer.getTopicDelay("topic3"));
    }

    @Test
    public void testRemoveTopicDelay() {
        delayProducer = new DelayProducer<>(mockProducer);
        
        delayProducer.setTopicDelay("topic1", 1000L);
        assertEquals(Long.valueOf(1000L), delayProducer.getTopicDelay("topic1"));
        
        // 移除配置
        Long removed = delayProducer.removeTopicDelay("topic1");
        assertEquals(Long.valueOf(1000L), removed);
        assertNull(delayProducer.getTopicDelay("topic1"));
        
        // 移除不存在的配置
        Long removedNull = delayProducer.removeTopicDelay("nonexistent");
        assertNull(removedNull);
    }

    @Test
    public void testGetAllTopicDelays() {
        delayProducer = new DelayProducer<>(mockProducer);
        
        delayProducer.setTopicDelay("topic1", 1000L);
        delayProducer.setTopicDelay("topic2", 2000L);
        
        Map<String, Long> allDelays = delayProducer.getAllTopicDelays();
        assertEquals(2, allDelays.size());
        assertEquals(Long.valueOf(1000L), allDelays.get("topic1"));
        assertEquals(Long.valueOf(2000L), allDelays.get("topic2"));
        
        // 验证返回的是副本，修改不会影响原始配置
        allDelays.put("topic3", 3000L);
        assertNull(delayProducer.getTopicDelay("topic3"));
    }

    @Test
    public void testSendWithDelayStillWorks() throws Exception {
        delayProducer = new DelayProducer<>(mockProducer);
        
        // 原有的sendWithDelay方法仍然可用
        Future<RecordMetadata> future = delayProducer.sendWithDelay("test-topic", "key1", "value1", 5000L);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        String delayMs = new String(record.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        assertEquals("5000", delayMs);
    }

    @Test
    public void testSendDeliverAtStillWorks() throws Exception {
        delayProducer = new DelayProducer<>(mockProducer);
        
        long deliverAt = System.currentTimeMillis() + 10000;
        
        // 原有的sendDeliverAt方法仍然可用
        Future<RecordMetadata> future = delayProducer.sendDeliverAt("test-topic", "key1", "value1", deliverAt);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertNotNull(record.headers().lastHeader("d2k-deliver-at"));
        String deliverAtStr = new String(record.headers().lastHeader("d2k-deliver-at").value(), StandardCharsets.UTF_8);
        assertEquals(String.valueOf(deliverAt), deliverAtStr);
    }
}