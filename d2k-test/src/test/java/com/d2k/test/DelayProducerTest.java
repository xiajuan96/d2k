package com.d2k.test;

import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * DelayProducer 单元测试 - 重构后的简化版本
 * @author xiajuan96
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
        
        delayProducer = new DelayProducer<>(mockProducer);
    }

    @Test
    public void testCreateDelayProducer() {
        assertNotNull(delayProducer);
    }

    @Test
    public void testSendWithDelay() throws Exception {
        // 使用sendWithDelay发送消息
        Future<RecordMetadata> future = delayProducer.sendWithDelay("test-topic", "key1", "value1", 3000L);
        
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
    public void testSendWithDelayWithoutKey() throws Exception {
        // 使用sendWithDelay发送消息（无键）
        Future<RecordMetadata> future = delayProducer.sendWithDelay("test-topic", null, "value1", 2000L);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());
        assertEquals("value1", record.value());
        
        // 验证延迟头部
        assertNotNull(record.headers().lastHeader("d2k-delay-ms"));
        String delayMs = new String(record.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        assertEquals("2000", delayMs);
    }

    @Test
    public void testSendDeliverAt() throws Exception {
        long deliverAt = System.currentTimeMillis() + 5000L;
        
        // 使用sendDeliverAt发送消息
        Future<RecordMetadata> future = delayProducer.sendDeliverAt("test-topic", "key1", "value1", deliverAt);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertEquals("key1", record.key());
        assertEquals("value1", record.value());
        
        // 验证投递时间头部
        assertNotNull(record.headers().lastHeader("d2k-deliver-at"));
        String deliverAtHeader = new String(record.headers().lastHeader("d2k-deliver-at").value(), StandardCharsets.UTF_8);
        assertEquals(String.valueOf(deliverAt), deliverAtHeader);
    }

    @Test
    public void testSendDeliverAtWithoutKey() throws Exception {
        long deliverAt = System.currentTimeMillis() + 4000L;
        
        // 使用sendDeliverAt发送消息（无键）
        Future<RecordMetadata> future = delayProducer.sendDeliverAt("test-topic", null, "value1", deliverAt);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());
        assertEquals("value1", record.value());
        
        // 验证投递时间头部
        assertNotNull(record.headers().lastHeader("d2k-deliver-at"));
        String deliverAtHeader = new String(record.headers().lastHeader("d2k-deliver-at").value(), StandardCharsets.UTF_8);
        assertEquals(String.valueOf(deliverAt), deliverAtHeader);
    }

    @Test
    public void testSendWithDelayNegativeDelay() throws Exception {
        // DelayProducer使用Math.max(0L, delayMs)处理负数延迟，将其转换为0
        Future<RecordMetadata> future = delayProducer.sendWithDelay("test-topic", "key1", "value1", -1000L);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        // 验证延迟头部被设置为0
        assertNotNull(record.headers().lastHeader("d2k-delay-ms"));
        String delayMs = new String(record.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        assertEquals("0", delayMs);
    }

    @Test
    public void testSendDeliverAtPastTime() throws Exception {
        long pastTime = System.currentTimeMillis() - 1000L;
        
        // DelayProducer没有对过去时间进行验证，直接发送
        Future<RecordMetadata> future = delayProducer.sendDeliverAt("test-topic", "key1", "value1", pastTime);
        
        assertNotNull(future);
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        // 验证投递时间头部
        assertNotNull(record.headers().lastHeader("d2k-deliver-at"));
        String deliverAtHeader = new String(record.headers().lastHeader("d2k-deliver-at").value(), StandardCharsets.UTF_8);
        assertEquals(String.valueOf(pastTime), deliverAtHeader);
    }
}