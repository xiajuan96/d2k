/**
 * @author xiajuan96
 */
package com.d2k.config;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试DelayConfig和DelayConfigBuilder中topic互斥性验证功能
 */
public class DelayConfigTopicMutualExclusionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testDelayConfigConstructorWithConflictingTopics() {
        Map<String, Long> topicDelays = new HashMap<>();
        topicDelays.put("test-topic", 1000L);
        
        Map<TopicPartition, Long> topicPartitionDelays = new HashMap<>();
        topicPartitionDelays.put(new TopicPartition("test-topic", 0), 2000L);
        
        // 应该抛出IllegalArgumentException
        new DelayConfig(topicDelays, topicPartitionDelays);
    }
    
    @Test
    public void testDelayConfigConstructorWithNonConflictingTopics() {
        Map<String, Long> topicDelays = new HashMap<>();
        topicDelays.put("topic-a", 1000L);
        
        Map<TopicPartition, Long> topicPartitionDelays = new HashMap<>();
        topicPartitionDelays.put(new TopicPartition("topic-b", 0), 2000L);
        
        // 应该成功创建
        DelayConfig config = new DelayConfig(topicDelays, topicPartitionDelays);
        Assert.assertNotNull(config);
        Assert.assertEquals(Long.valueOf(1000L), config.getTopicDelay("topic-a"));
        Assert.assertEquals(Long.valueOf(2000L), config.getTopicPartitionDelay("topic-b", 0));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicDelayWithExistingPartitionConfig() {
        DelayConfig config = new DelayConfig();
        config.setTopicPartitionDelay("test-topic", 0, 1000L);
        
        // 应该抛出IllegalArgumentException
        config.setTopicDelay("test-topic", 2000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicPartitionDelayWithExistingTopicConfig() {
        DelayConfig config = new DelayConfig();
        config.setTopicDelay("test-topic", 1000L);
        
        // 应该抛出IllegalArgumentException
        config.setTopicPartitionDelay("test-topic", 0, 2000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDelayConfigBuilderWithConflictingTopics() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        builder.withTopicDelay("test-topic", 1000L);
        builder.withTopicPartitionDelay("test-topic", 0, 2000L);
        
        // 应该在build时抛出IllegalArgumentException
        builder.build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDelayConfigBuilderWithTopicDelayAfterPartition() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        builder.withTopicPartitionDelay("test-topic", 0, 1000L);
        
        // 应该立即抛出IllegalArgumentException
        builder.withTopicDelay("test-topic", 2000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDelayConfigBuilderWithPartitionDelayAfterTopic() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        builder.withTopicDelay("test-topic", 1000L);
        
        // 应该立即抛出IllegalArgumentException
        builder.withTopicPartitionDelay("test-topic", 0, 2000L);
    }
    
    @Test
    public void testDelayConfigBuilderWithNonConflictingTopics() {
        DelayConfigBuilder builder = new DelayConfigBuilder();
        builder.withTopicDelay("topic-a", 1000L);
        builder.withTopicPartitionDelay("topic-b", 0, 2000L);
        
        DelayConfig config = builder.build();
        Assert.assertNotNull(config);
        Assert.assertEquals(Long.valueOf(1000L), config.getTopicDelay("topic-a"));
        Assert.assertEquals(Long.valueOf(2000L), config.getTopicPartitionDelay("topic-b", 0));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicDelaysWithConflictingTopics() {
        DelayConfig config = new DelayConfig();
        config.setTopicPartitionDelay("test-topic", 0, 1000L);
        
        Map<String, Long> topicDelays = new HashMap<>();
        topicDelays.put("test-topic", 2000L);
        
        // 应该抛出IllegalArgumentException
        config.setTopicDelays(topicDelays);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicPartitionDelaysWithConflictingTopics() {
        DelayConfig config = new DelayConfig();
        config.setTopicDelay("test-topic", 1000L);
        
        Map<TopicPartition, Long> partitionDelays = new HashMap<>();
        partitionDelays.put(new TopicPartition("test-topic", 0), 2000L);
        
        // 应该抛出IllegalArgumentException
        config.setTopicPartitionDelays(partitionDelays);
    }
}