package com.d2k.test;

import com.d2k.config.DelayConfig;
import com.d2k.config.DelayConfigBuilder;
import com.d2k.producer.ConfigurableDelayProducer;
import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * ConfigurableDelayProducer 测试类
 * 
 * @author xiajuan96
 */
public class ConfigurableDelayProducerTest {
    
    private MockProducer<String, String> mockProducer;
    private DelayProducer<String, String> delayProducer;
    private DelayConfig delayConfig;
    private ConfigurableDelayProducer<String, String> configurableDelayProducer;
    
    @Before
    public void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        delayProducer = new DelayProducer<>(mockProducer);
        
        // 创建延迟配置
        delayConfig = new DelayConfigBuilder()
                .withTopicPartitionDelay("test-topic", 0, 1000L)
                .withTopicPartitionDelay("test-topic", 1, 2000L)
                .withTopicPartitionDelay("test-topic", 2, 1000L)
                .withTopicPartitionDelay("test-topic", 3, 3000L)
                .withTopicPartitionDelay("another-topic", 0, 1000L)
                .build();
        
        configurableDelayProducer = new ConfigurableDelayProducer<>(delayProducer, delayConfig);
    }
    
    @Test
    public void testSendToRandomPartitionWithKey() throws Exception {
        // 测试发送到延迟时间为1000ms的分区（应该从分区0和2中随机选择）
        Future<RecordMetadata> future = configurableDelayProducer.sendWithDelayMs(
                "test-topic", "test-key", "test-value", 1000L);
        
        assertNotNull(future);
        
        // 验证消息已发送
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertEquals("test-key", record.key());
        assertEquals("test-value", record.value());
        
        // 验证分区是0或2（配置了1000ms延迟的分区）
        int partition = record.partition();
        assertTrue("Partition should be 0 or 2", partition == 0 || partition == 2);
        
        // 验证延迟时间头部
        assertEquals("1000", new String(record.headers().lastHeader("d2k-delay-ms").value()));
    }
    
    @Test
    public void testSendToRandomPartitionWithoutKey() throws Exception {
        // 测试发送到延迟时间为2000ms的分区（应该选择分区1）
        Future<RecordMetadata> future = configurableDelayProducer.sendWithDelayMs(
                "test-topic", "test-value", 2000L);
        
        assertNotNull(future);
        
        // 验证消息已发送
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());
        assertEquals("test-value", record.value());
        assertEquals(1, record.partition().intValue()); // 只有分区1配置了2000ms延迟
        
        // 验证延迟时间头部
        assertEquals("2000", new String(record.headers().lastHeader("d2k-delay-ms").value()));
    }
    
    @Test
    public void testSendToRandomPartitionUniqueDelay() throws Exception {
        // 测试发送到延迟时间为3000ms的分区（应该选择分区3）
        Future<RecordMetadata> future = configurableDelayProducer.sendWithDelayMs(
                "test-topic", "test-key", "test-value", 3000L);
        
        assertNotNull(future);
        
        // 验证消息已发送
        assertEquals(1, mockProducer.history().size());
        
        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("test-topic", record.topic());
        assertEquals(3, record.partition().intValue()); // 只有分区3配置了3000ms延迟
        
        // 验证延迟时间头部
        assertEquals("3000", new String(record.headers().lastHeader("d2k-delay-ms").value()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSendToRandomPartitionNoMatchingPartitions() {
        // 测试发送到没有配置的延迟时间（应该抛出异常）
        configurableDelayProducer.sendWithDelayMs(
                "test-topic", "test-key", "test-value", 5000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSendToRandomPartitionNonExistentTopic() {
        // 测试发送到不存在的topic（应该抛出异常）
        configurableDelayProducer.sendWithDelayMs(
                "non-existent-topic", "test-key", "test-value", 1000L);
    }
    
    @Test
    public void testMultipleSendsRandomDistribution() throws Exception {
        // 测试多次发送，验证随机分布（发送到延迟时间为1000ms的分区）
        int sendCount = 10;
        boolean foundPartition0 = false;
        boolean foundPartition2 = false;
        
        for (int i = 0; i < sendCount; i++) {
            configurableDelayProducer.sendWithDelayMs(
                    "test-topic", "key-" + i, "value-" + i, 1000L);
            
            ProducerRecord<String, String> record = mockProducer.history().get(i);
            int partition = record.partition();
            
            if (partition == 0) {
                foundPartition0 = true;
            } else if (partition == 2) {
                foundPartition2 = true;
            } else {
                fail("Unexpected partition: " + partition + ". Should be 0 or 2.");
            }
        }
        
        // 验证总共发送了指定数量的消息
        assertEquals(sendCount, mockProducer.history().size());
        
        // 注意：由于是随机选择，不能保证一定会使用到所有分区，
        // 但在多次发送的情况下，通常会分布到不同分区
        // 这里只验证所有分区都是有效的（0或2）
    }
    
    @Test
    public void testGetDelayConfig() {
        DelayConfig retrievedConfig = configurableDelayProducer.getDelayConfig();
        assertSame(delayConfig, retrievedConfig);
    }
    
    @Test
    public void testGetDelayProducer() {
        DelayProducer<String, String> retrievedProducer = configurableDelayProducer.getDelayProducer();
        assertSame(delayProducer, retrievedProducer);
    }
    
    @Test
    public void testSendWithTopicLevelDelay() throws Exception {
        // 创建包含topic级别延迟配置的DelayConfig
        DelayConfig topicLevelConfig = new DelayConfigBuilder()
                .withTopicDelay("topic-with-default", 5000L)
                .withTopicPartitionDelay("partition-topic", 0, 1000L) // 分区0有特定配置
                .build();
        
        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, topicLevelConfig);
        
        // 测试发送到延迟时间为5000ms的消息（应该匹配topic级别配置）
        // 由于MockProducer没有真实的分区信息，这个测试主要验证逻辑不会抛出异常
        try {
            Future<RecordMetadata> future = producer.sendWithDelayMs(
                    "topic-with-default", "test-key", "test-value", 5000L);
            
            // 如果没有可用分区，应该抛出IllegalArgumentException
            // 在MockProducer环境下，partitionsFor会返回null，所以会抛出异常
        } catch (IllegalArgumentException e) {
            // 这是预期的，因为MockProducer没有分区信息
            assertTrue(e.getMessage().contains("has no partitions configured with delay time"));
        }
    }
    
    @Test
    public void testClose() {
        // 测试关闭方法不抛出异常
        configurableDelayProducer.close();
        // MockProducer的close方法不会抛出异常，这里主要测试方法调用
    }
}