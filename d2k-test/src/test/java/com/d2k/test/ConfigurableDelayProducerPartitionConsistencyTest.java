package com.d2k.test;

import com.d2k.config.DelayConfig;
import com.d2k.config.DelayConfigBuilder;
import com.d2k.producer.ConfigurableDelayProducer;
import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author xiajuan96
 * 测试ConfigurableDelayProducer中topic内partition延迟时间一致性检查功能
 */
public class ConfigurableDelayProducerPartitionConsistencyTest {

    private Producer<String, String> mockProducer;
    private DelayProducer<String, String> delayProducer;

    @Before
    public void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        delayProducer = new DelayProducer<>(mockProducer);
    }

    @Test
    public void testConsistentPartitionDelays() {
        // 创建一致的partition延迟配置
        DelayConfig config = new DelayConfigBuilder()
                .withTopicDelay("test-topic", 1000L)
                .withTopicPartitionDelay("consistent-topic", 0, 1000L)
                .withTopicPartitionDelay("consistent-topic", 1, 1000L)
                .build();

        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, config);

        // 测试发送到有topic级别配置的topic，应该正常发送
        try {
            producer.send("test-topic", "key", "value");
        } catch (IllegalArgumentException e) {
            fail("Should not throw exception for topic with delay configuration: " + e.getMessage());
        }
    }

    @Test
    public void testInconsistentPartitionDelays() {
        // 创建不一致的partition延迟配置
        DelayConfig config = new DelayConfigBuilder()
                .withTopicDelay("test-topic", 1000L)
                .withTopicPartitionDelay("inconsistent-topic", 0, 1000L)
                .withTopicPartitionDelay("inconsistent-topic", 1, 2000L)
                .build();

        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, config);

        // 应该抛出IllegalArgumentException
        try {
            producer.send("inconsistent-topic", "key", "value");
            fail("Should throw IllegalArgumentException for inconsistent partition delays");
        } catch (IllegalArgumentException e) {
            assertTrue("Error message should contain 'inconsistent'", e.getMessage().contains("inconsistent"));
        }
    }

    @Test
    public void testSinglePartitionDelay() {
        // 只有一个partition配置，应该正常工作
        DelayConfig config = new DelayConfigBuilder()
                .withTopicDelay("test-topic", 1000L)
                .withTopicPartitionDelay("single-topic", 0, 2000L)
                .build();

        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, config);

        // 应该正常发送，不抛出异常
        try {
            producer.send("test-topic", "key", "value");
        } catch (IllegalArgumentException e) {
            fail("Should not throw exception for single partition delay: " + e.getMessage());
        }
    }

    @Test
    public void testNoPartitionDelayConfiguration() {
        // 没有partition级别配置，只有topic级别配置
        DelayConfig config = new DelayConfigBuilder()
                .withTopicDelay("test-topic", 1000L)
                .build();

        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, config);

        // 应该正常发送，不抛出异常
        try {
            producer.send("test-topic", "key", "value");
        } catch (IllegalArgumentException e) {
            fail("Should not throw exception when no partition delay configuration: " + e.getMessage());
        }
    }

    @Test
    public void testInconsistentPartitionDelaysErrorMessage() {
        // 测试错误消息的格式
        DelayConfig config = new DelayConfigBuilder()
                .withTopicDelay("test-topic", 1000L)
                .withTopicPartitionDelay("error-topic", 0, 1000L)
                .withTopicPartitionDelay("error-topic", 1, 2000L)
                .withTopicPartitionDelay("error-topic", 2, 3000L)
                .build();

        ConfigurableDelayProducer<String, String> producer = 
                new ConfigurableDelayProducer<>(delayProducer, config);

        try {
            producer.send("error-topic", "key", "value");
            fail("Should throw IllegalArgumentException for inconsistent partition delays");
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            assertTrue("Error message should contain topic name", message.contains("error-topic"));
            assertTrue("Error message should contain 'inconsistent'", message.contains("inconsistent"));
            assertTrue("Error message should contain partition info", message.contains("partition-"));
        }
    }
}