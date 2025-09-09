/**
 * @author xiajuan96
 */
package com.d2k.test;

import com.d2k.config.DelayConfig;
import com.d2k.config.DelayConfigBuilder;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * DelayConfigBuilder 单元测试
 */
public class DelayConfigBuilderTest {
    
    private DelayConfigBuilder builder;
    
    @Before
    public void setUp() {
        builder = DelayConfigBuilder.newBuilder();
    }
    
    @Test
    public void testBasicChainedCalls() {
        DelayConfig config = builder
                .withTopicDelay("order-topic", 5000L)
                .withTopicDelay("payment-topic", 3000L)
                .withTopicPartitionDelay("inventory-topic", 0, 2000L)
                .withTopicPartitionDelay("inventory-topic", 1, 8000L)
                .build();
        
        assertNotNull(config);
        assertEquals(2, config.getAllTopicDelays().size());
        assertEquals(2, config.getAllTopicPartitionDelays().size());
        
        assertEquals(Long.valueOf(5000L), config.getTopicDelay("order-topic"));
        assertEquals(Long.valueOf(3000L), config.getTopicDelay("payment-topic"));
        assertEquals(Long.valueOf(2000L), config.getTopicPartitionDelay("inventory-topic", 0));
        assertEquals(Long.valueOf(8000L), config.getTopicPartitionDelay("inventory-topic", 1));
    }
    
    @Test
    public void testBatchConfiguration() {
        Map<String, Long> topicDelays = new HashMap<>();
        topicDelays.put("user-topic", 1000L);
        topicDelays.put("notification-topic", 2000L);
        
        Map<TopicPartition, Long> topicPartitionDelays = new HashMap<>();
        topicPartitionDelays.put(new TopicPartition("analytics-topic", 0), 500L);
        topicPartitionDelays.put(new TopicPartition("analytics-topic", 1), 1500L);
        
        DelayConfig config = builder
                .withTopicDelays(topicDelays)
                .withTopicPartitionDelays(topicPartitionDelays)
                .build();
        
        assertEquals(2, config.getAllTopicDelays().size());
        assertEquals(2, config.getAllTopicPartitionDelays().size());
        
        assertEquals(Long.valueOf(1000L), config.getTopicDelay("user-topic"));
        assertEquals(Long.valueOf(2000L), config.getTopicDelay("notification-topic"));
    }
    
    @Test
    public void testCopyFromExistingConfig() {
        DelayConfig originalConfig = DelayConfigBuilder.newBuilder()
                .withTopicDelay("original-topic", 4000L)
                .withTopicPartitionDelay("partition-topic", 0, 1000L)
                .build();
        
        DelayConfig newConfig = DelayConfigBuilder.fromDelayConfig(originalConfig)
                .withTopicDelay("new-topic", 3000L)
                .build();
        
        assertEquals(2, newConfig.getAllTopicDelays().size());
        assertEquals(1, newConfig.getAllTopicPartitionDelays().size());
        
        assertEquals(Long.valueOf(4000L), newConfig.getTopicDelay("original-topic"));
        assertEquals(Long.valueOf(3000L), newConfig.getTopicDelay("new-topic"));
        assertEquals(Long.valueOf(1000L), newConfig.getTopicPartitionDelay("partition-topic", 0));
    }
    
    @Test
    public void testBuilderStateManagement() {
        builder.withTopicDelay("test-topic", 1000L);
        assertEquals(1, builder.getTopicConfigCount());
        assertEquals(0, builder.getTopicPartitionConfigCount());
        assertTrue(builder.hasAnyConfiguration());
        
        builder.withTopicPartitionDelay("state-topic", 0, 500L);
        assertEquals(1, builder.getTopicConfigCount());
        assertEquals(1, builder.getTopicPartitionConfigCount());
        
        builder.clear();
        assertEquals(0, builder.getTopicConfigCount());
        assertEquals(0, builder.getTopicPartitionConfigCount());
        assertFalse(builder.hasAnyConfiguration());
    }
    
    @Test
    public void testRemoveOperations() {
        DelayConfig config = builder
                .withTopicDelay("topic1", 1000L)
                .withTopicDelay("topic2", 2000L)
                .withTopicPartitionDelay("topic3", 0, 500L)
                .removeTopicDelay("topic2")
                .removeTopicPartitionDelay("topic3", 0)
                .build();
        
        assertEquals(1, config.getAllTopicDelays().size());
        assertEquals(0, config.getAllTopicPartitionDelays().size());
        assertEquals(Long.valueOf(1000L), config.getTopicDelay("topic1"));
        assertNull(config.getTopicDelay("topic2"));
    }
    
    @Test
    public void testClearOperations() {
        builder.withTopicDelay("topic1", 1000L)
               .withTopicPartitionDelay("clear-topic", 0, 500L);
        
        builder.clearTopicDelays();
        assertEquals(0, builder.getTopicConfigCount());
        assertEquals(1, builder.getTopicPartitionConfigCount());
        
        builder.withTopicDelay("topic2", 2000L);
        builder.clearTopicPartitionDelays();
        assertEquals(1, builder.getTopicConfigCount());
        assertEquals(0, builder.getTopicPartitionConfigCount());
    }
    
    @Test
    public void testDelayConfigurationPriority() {
        DelayConfig config = builder
                .withTopicDelay("priority-topic", 5000L)
                .withTopicPartitionDelay("partition-priority-topic", 0, 2000L)
                .build();
        
        // 分区级别配置优先
        assertEquals(Long.valueOf(2000L), config.getDelay("partition-priority-topic", 0));
        // 没有分区配置时使用主题级别配置
        assertEquals(Long.valueOf(5000L), config.getDelay("priority-topic", 1));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullTopicValidation() {
        builder.withTopicDelay(null, 1000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTopicValidation() {
        builder.withTopicDelay("", 1000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNegativeDelayValidation() {
        builder.withTopicDelay("test-topic", -1000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullDelayValidation() {
        Map<String, Long> mapWithNull = new HashMap<>();
        mapWithNull.put("test-topic", null);
        builder.withTopicDelays(mapWithNull);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNegativePartitionValidation() {
        builder.withTopicPartitionDelay("test-topic", -1, 1000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullTopicDelayMapValidation() {
        builder.withTopicDelays(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullTopicPartitionDelayMapValidation() {
        builder.withTopicPartitionDelays(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPartitionValidation() {
        // 测试负数分区号
        builder.withTopicPartitionDelay("test-topic", -1, 1000L);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNullDelayConfigCopy() {
        DelayConfigBuilder.fromDelayConfig(null);
    }
    
    @Test
    public void testBuilderReuse() {
        DelayConfig config1 = builder
                .withTopicDelay("topic1", 1000L)
                .build();
        
        // Builder可以重复使用
        DelayConfig config2 = builder
                .withTopicDelay("topic2", 2000L)
                .build();
        
        // 第一个配置不受影响
        assertEquals(Long.valueOf(1000L), config1.getTopicDelay("topic1"));
        assertNull(config1.getTopicDelay("topic2"));
        
        // 第二个配置包含所有设置
        assertEquals(Long.valueOf(1000L), config2.getTopicDelay("topic1"));
        assertEquals(Long.valueOf(2000L), config2.getTopicDelay("topic2"));
    }
    
    @Test
    public void testThreadSafety() {
        DelayConfig config = builder
                .withTopicDelay("topic1", 1000L)
                .withTopicPartitionDelay("thread-topic", 0, 500L)
                .build();
        
        // 验证返回的Map是副本，修改不会影响原配置
        Map<String, Long> topicDelays = config.getAllTopicDelays();
        topicDelays.put("new-topic", 3000L);
        
        assertNull(config.getTopicDelay("new-topic"));
        assertEquals(1, config.getAllTopicDelays().size());
    }
    
    @Test
    public void testStaticFactoryMethods() {
        DelayConfigBuilder newBuilder = DelayConfigBuilder.newBuilder();
        assertNotNull(newBuilder);
        assertFalse(newBuilder.hasAnyConfiguration());
        
        DelayConfig existingConfig = DelayConfigBuilder.newBuilder()
                .withTopicDelay("existing-topic", 2000L)
                .build();
        
        DelayConfigBuilder fromExisting = DelayConfigBuilder.fromDelayConfig(existingConfig);
        assertNotNull(fromExisting);
        assertTrue(fromExisting.hasAnyConfiguration());
        assertEquals(1, fromExisting.getTopicConfigCount());
    }
}