/**
 * @author xiajuan96
 */
package com.d2k.producer;

import com.d2k.config.DelayConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 可配置的延迟消息生产者
 * 基于DelayConfig配置发送延迟消息，不支持自定义延迟时间
 * 通过DelayProducer实例进行延迟消息发送
 */
public class ConfigurableDelayProducer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(ConfigurableDelayProducer.class);
    
    private final DelayProducer<K, V> delayProducer;
    private final DelayConfig delayConfig;
    private final Map<String, Integer> topicPartitionsCache;
    
    /**
     * 使用配置属性创建ConfigurableDelayProducer
     * 
     * @param props 生产者配置属性
     * @param delayConfig 延迟配置
     */
    public ConfigurableDelayProducer(Map<String, Object> props, DelayConfig delayConfig) {
        log.info("Initializing ConfigurableDelayProducer with properties: {} and delayConfig", props);
        this.delayProducer = new DelayProducer<>(props);
        this.delayConfig = delayConfig;
        this.topicPartitionsCache = initializeTopicPartitionsCache();
    }
    
    /**
     * 使用现有Producer和DelayConfig创建ConfigurableDelayProducer
     * 
     * @param producer Kafka生产者实例
     * @param delayConfig 延迟配置
     */
    public ConfigurableDelayProducer(Producer<K, V> producer, DelayConfig delayConfig) {
        log.info("Initializing ConfigurableDelayProducer with provided producer and delayConfig");
        this.delayProducer = new DelayProducer<>(producer);
        this.delayConfig = delayConfig;
        this.topicPartitionsCache = initializeTopicPartitionsCache();
    }
    
    /**
     * 使用现有DelayProducer和DelayConfig创建ConfigurableDelayProducer
     * 
     * @param delayProducer DelayProducer实例
     * @param delayConfig 延迟配置
     */
    public ConfigurableDelayProducer(DelayProducer<K, V> delayProducer, DelayConfig delayConfig) {
        log.info("Initializing ConfigurableDelayProducer with provided delayProducer and delayConfig");
        this.delayProducer = delayProducer;
        this.delayConfig = delayConfig;
        this.topicPartitionsCache = initializeTopicPartitionsCache();
    }
    
    /**
     * 发送延迟消息（使用配置的延迟时间）
     * 优先使用topic-partition级别配置，如果没有则使用topic级别配置
     * 
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果topic没有配置延迟时间或topic内各partition延迟时间不一致
     */
    public Future<RecordMetadata> send(String topic, K key, V value) {
        // 检查topic内各partition的延迟时间是否一致
        validateTopicPartitionDelayConsistency(topic);
        
        // 优先使用topic级别配置
        Long delayMs = delayConfig.getTopicDelay(topic);
        if (delayMs == null) {
            throw new IllegalArgumentException("Topic '" + topic + "' has no delay configuration. Please configure delay time for this topic.");
        }
        return delayProducer.sendWithDelay(topic, key, value, delayMs);
    }
    
    /**
     * 发送延迟消息（使用配置的延迟时间，无键）
     * 
     * @param topic 主题
     * @param value 消息值
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果topic没有配置延迟时间
     */
    public Future<RecordMetadata> send(String topic, V value) {
        return send(topic, null, value);
    }
    
    /**
     * 发送延迟消息到随机分区（根据指定延迟时间选择匹配的分区）
     * 从配置了指定延迟时间的分区中随机选择一个进行发送
     * 
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果topic没有配置或没有匹配延迟时间的分区
     */
    public Future<RecordMetadata> sendWithDelayMs(String topic, K key, V value, long delayMs) {
        // 获取所有匹配延迟时间的分区
        List<Integer> matchingPartitions = getPartitionsWithDelay(topic, delayMs);
        
        if (matchingPartitions.isEmpty()) {
            throw new IllegalArgumentException("Topic '" + topic + "' has no partitions configured with delay time " + delayMs + "ms. Please configure delay time for topic-partitions.");
        }
        
        // 随机选择一个分区
        Random random = new Random();
        int selectedPartition = matchingPartitions.get(random.nextInt(matchingPartitions.size()));
        
        log.debug("Selected partition {} from {} matching partitions for topic {} with delay {}ms", 
                selectedPartition, matchingPartitions.size(), topic, delayMs);
        
        return delayProducer.sendWithDelayToPartition(topic, selectedPartition, key, value, delayMs);
    }
    
    /**
     * 发送延迟消息到随机分区（根据指定延迟时间选择匹配的分区，无键）
     * 
     * @param topic 主题
     * @param value 消息值
     * @param delayMs 延迟时间（毫秒）
     * @return Future<RecordMetadata>
     * @throws IllegalArgumentException 如果topic没有配置或没有匹配延迟时间的分区
     */
    public Future<RecordMetadata> sendWithDelayMs(String topic, V value, long delayMs) {
        return sendWithDelayMs(topic, null, value, delayMs);
    }
    
    /**
     * 初始化topic分区缓存
     * 在构造函数中调用，预先获取所有相关topic的分区信息
     * 
     * @return topic到分区数量的映射
     */
    private Map<String, Integer> initializeTopicPartitionsCache() {
        Map<String, Integer> cache = new HashMap<>();
        
        // 获取所有配置的topic
        Set<String> allTopics = new HashSet<>();
        
        // 从topic级别配置中获取topic
        Map<String, Long> allTopicDelays = delayConfig.getAllTopicDelays();
        allTopics.addAll(allTopicDelays.keySet());
        
        // 从topic-partition级别配置中获取topic
        Map<org.apache.kafka.common.TopicPartition, Long> allTopicPartitionDelays = delayConfig.getAllTopicPartitionDelays();
        for (org.apache.kafka.common.TopicPartition topicPartition : allTopicPartitionDelays.keySet()) {
            allTopics.add(topicPartition.topic());
        }
        
        // 为每个topic获取分区信息
        for (String topic : allTopics) {
            try {
                List<org.apache.kafka.common.PartitionInfo> partitionInfos = 
                    delayProducer.getProducer().partitionsFor(topic);
                
                if (partitionInfos != null) {
                    int partitionCount = partitionInfos.size();
                    cache.put(topic, partitionCount);
                    log.info("Cached {} partitions for topic {}", partitionCount, topic);
                } else {
                    log.warn("No partition info found for topic {}", topic);
                    cache.put(topic, 0);
                }
            } catch (Exception e) {
                log.warn("Failed to get partition info for topic {}: {}", topic, e.getMessage());
                cache.put(topic, 0);
            }
        }
        
        return cache;
    }
    
    /**
     * 获取配置了指定延迟时间的分区列表
     * 
     * @param topic 主题
     * @param delayMs 延迟时间（毫秒）
     * @return 匹配的分区号列表
     */
    private List<Integer> getPartitionsWithDelay(String topic, long delayMs) {
        List<Integer> matchingPartitions = new ArrayList<>();
        
        // 获取所有topic-partition级别的配置
        Map<org.apache.kafka.common.TopicPartition, Long> allTopicPartitionDelays = delayConfig.getAllTopicPartitionDelays();
        Set<Integer> configuredPartitions = new HashSet<>();
        
        // 查找匹配的分区
        for (Map.Entry<org.apache.kafka.common.TopicPartition, Long> entry : allTopicPartitionDelays.entrySet()) {
            org.apache.kafka.common.TopicPartition topicPartition = entry.getKey();
            Long configuredDelay = entry.getValue();
            
            // 检查是否是当前topic的分区
            if (topicPartition.topic().equals(topic)) {
                int partition = topicPartition.partition();
                configuredPartitions.add(partition);
                
                // 如果延迟时间匹配，添加到结果列表
                if (Objects.equals(configuredDelay, delayMs)) {
                    matchingPartitions.add(partition);
                }
            }
        }
        
        // 检查topic级别的延迟配置
        Map<String, Long> allTopicDelays = delayConfig.getAllTopicDelays();
        Long topicDelay = allTopicDelays.get(topic);
        
        if (topicDelay != null && Objects.equals(topicDelay, delayMs)) {
            // 如果topic级别的延迟时间匹配，使用缓存的分区数量信息
            Integer cachedPartitionCount = topicPartitionsCache.get(topic);
            if (cachedPartitionCount != null && cachedPartitionCount > 0) {
                for (int partition = 0; partition < cachedPartitionCount; partition++) {
                    // 只有未在topic-partition级别配置的分区才使用topic级别的延迟
                    if (!configuredPartitions.contains(partition)) {
                        matchingPartitions.add(partition);
                    }
                }
            } else {
                log.warn("No cached partition info found for topic {} or partition count is 0", topic);
            }
        }
        
        log.debug("Found {} partitions with delay {}ms for topic {}: {}", 
                matchingPartitions.size(), delayMs, topic, matchingPartitions);
        
        return matchingPartitions;
    }

    /**
     * 验证topic内各partition的延迟时间是否一致
     * 
     * @param topic 主题名称
     * @throws IllegalArgumentException 如果topic内各partition延迟时间不一致
     */
    private void validateTopicPartitionDelayConsistency(String topic) {
        Map<TopicPartition, Long> allTopicPartitionDelays = delayConfig.getAllTopicPartitionDelays();
        log.debug("Validating partition delay consistency for topic: {}, all partition delays: {}", topic, allTopicPartitionDelays);
        
        // 获取该topic下所有partition的延迟配置
        Map<Integer, Long> topicPartitionDelays = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : allTopicPartitionDelays.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            if (topic.equals(topicPartition.topic())) {
                topicPartitionDelays.put(topicPartition.partition(), entry.getValue());
            }
        }
        
        log.debug("Found {} partition delays for topic {}: {}", topicPartitionDelays.size(), topic, topicPartitionDelays);
        
        // 如果该topic下有多个partition配置，检查延迟时间是否一致
        if (topicPartitionDelays.size() > 1) {
            Set<Long> uniqueDelays = new HashSet<>(topicPartitionDelays.values());
            log.debug("Unique delay values: {}", uniqueDelays);
            if (uniqueDelays.size() > 1) {
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append("Topic '").append(topic).append("' has inconsistent partition delay configurations: ");
                for (Map.Entry<Integer, Long> entry : topicPartitionDelays.entrySet()) {
                    errorMsg.append("partition-").append(entry.getKey()).append("=").append(entry.getValue()).append("ms, ");
                }
                errorMsg.setLength(errorMsg.length() - 2); // 移除最后的", "
                log.error("Throwing exception: {}", errorMsg.toString());
                throw new IllegalArgumentException(errorMsg.toString());
            }
        }
    }

    /**
     * 获取延迟配置
     * 
     * @return DelayConfig
     */
    public DelayConfig getDelayConfig() {
        return delayConfig;
    }
    
    /**
     * 获取DelayProducer实例
     * 
     * @return DelayProducer实例
     */
    public DelayProducer<K, V> getDelayProducer() {
        return delayProducer;
    }
    
    /**
     * 关闭生产者
     */
    public void close() {
        log.info("Closing ConfigurableDelayProducer");
        delayProducer.close();
    }
}