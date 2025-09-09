/**
 * @author xiajuan96
 */
package com.d2k.config;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * DelayConfigBuilder bootstrap servers功能测试
 */
public class DelayConfigBuilderBootstrapTest {
    
    @Test
    public void testDefaultConstructor() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        assertNull(builder.getBootstrapServers());
    }
    
    @Test
    public void testConstructorWithBootstrapServers() {
        String bootstrapServers = "localhost:9092";
        DelayConfigBuilder builder = new DelayConfigBuilder(bootstrapServers);
        assertEquals(bootstrapServers, builder.getBootstrapServers());
    }
    
    @Test
    public void testBasicTopicValidationWithBootstrapServers() {
        DelayConfigBuilder builder = new DelayConfigBuilder("localhost:9092");
        
        // 正常的topic名称应该通过验证
        try {
            builder.withTopicDelay("test-topic", 1000L);
            // 如果没有抛出异常，测试通过
        } catch (Exception e) {
            fail("Should not throw exception for valid topic name: " + e.getMessage());
        }
    }
    
    @Test
    public void testInvalidTopicNameWithBootstrapServers() {
        DelayConfigBuilder builder = new DelayConfigBuilder("localhost:9092");
        
        // 包含null字符的topic名称应该抛出异常
        try {
            builder.withTopicDelay("test\0topic", 1000L);
            fail("Should throw IllegalArgumentException for topic with null character");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
        
        // 特殊名称"."应该抛出异常
        try {
            builder.withTopicDelay(".", 1000L);
            fail("Should throw IllegalArgumentException for topic name '.'");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
        
        // 特殊名称".."应该抛出异常
        try {
            builder.withTopicDelay("..", 1000L);
            fail("Should throw IllegalArgumentException for topic name '..'");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
    }
    
    @Test
    public void testTopicPartitionValidationWithBootstrapServers() {
        DelayConfigBuilder builder = new DelayConfigBuilder("localhost:9092");
        
        // 正常的topic-partition应该通过验证
        try {
            builder.withTopicPartitionDelay("test-topic", 0, 1000L);
        } catch (Exception e) {
            fail("Should not throw exception for valid topic-partition: " + e.getMessage());
        }
        
        // 负数分区应该抛出异常
        try {
            builder.withTopicPartitionDelay("test-topic", -1, 1000L);
            fail("Should throw IllegalArgumentException for negative partition");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
    }
    
    @Test
    public void testWithoutBootstrapServersValidation() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        
        // 没有bootstrap servers时，只进行基本验证
        try {
            builder.withTopicDelay("test-topic", 1000L);
        } catch (Exception e) {
            fail("Should not throw exception for basic topic validation: " + e.getMessage());
        }
        
        // 但是null或空字符串仍然会被检查
        try {
            builder.withTopicDelay(null, 1000L);
            fail("Should throw IllegalArgumentException for null topic");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
        
        try {
            builder.withTopicDelay("", 1000L);
            fail("Should throw IllegalArgumentException for empty topic");
        } catch (IllegalArgumentException e) {
            // 预期的异常
        }
    }
    
    @Test
    public void testToStringIncludesBootstrapServers() {
        DelayConfigBuilder builder1 = new DelayConfigBuilder();
        String toString1 = builder1.toString();
        assertTrue(toString1.contains("bootstrapServers='null'"));
        
        DelayConfigBuilder builder2 = new DelayConfigBuilder("localhost:9092");
        String toString2 = builder2.toString();
        assertTrue(toString2.contains("bootstrapServers='localhost:9092'"));
    }
    
    @Test
    public void testBuildWithBootstrapServers() {
        // 使用不存在的bootstrap servers来测试构建功能，但期望会抛出连接异常
        DelayConfigBuilder builder = new DelayConfigBuilder("localhost:9092");
        builder.withTopicDelay("test-topic", 1000L);
        builder.withTopicPartitionDelay("bootstrap-topic", 0, 2000L);
        
        try {
            DelayConfig config = builder.build();
            fail("Should throw IllegalArgumentException when cannot connect to Kafka cluster");
        } catch (IllegalArgumentException e) {
            // 预期的异常，因为无法连接到Kafka集群
            assertTrue(e.getMessage().contains("Timeout while validating topics with Kafka cluster") ||
                      e.getMessage().contains("Failed to validate topics with Kafka cluster"));
        }
    }
}