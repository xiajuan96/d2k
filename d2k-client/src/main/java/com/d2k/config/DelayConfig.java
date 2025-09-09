/**
 * @author xiajuan96
 */
package com.d2k.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 延迟配置类，支持按topic和topic-partition配置延迟时间
 */
public class DelayConfig {
    private static final Logger log = LoggerFactory.getLogger(DelayConfig.class);
    
    // topic级别的延迟配置（适用于该topic下所有partition）
    private final Map<String, Long> topicDelayConfig;
    
    // topic-partition级别的延迟配置，键为TopicPartition对象
    private final Map<TopicPartition, Long> topicPartitionDelayConfig;
    
    public DelayConfig() {
        this.topicDelayConfig = new ConcurrentHashMap<>();
        this.topicPartitionDelayConfig = new ConcurrentHashMap<>();
        log.debug("Created DelayConfig with empty configurations");
    }
    
    public DelayConfig(Map<String, Long> topicDelayConfig, Map<TopicPartition, Long> topicPartitionDelayConfig) {
        this.topicDelayConfig = new ConcurrentHashMap<>(topicDelayConfig != null ? topicDelayConfig : new ConcurrentHashMap<>());
        this.topicPartitionDelayConfig = new ConcurrentHashMap<>(topicPartitionDelayConfig != null ? topicPartitionDelayConfig : new ConcurrentHashMap<>());
        
        // 验证topic配置的互斥性
        validateTopicMutualExclusion();
        
        log.debug("Created DelayConfig with {} topic delays and {} topic-partition delays", 
                this.topicDelayConfig.size(), this.topicPartitionDelayConfig.size());
    }
    
    /**
     * 设置topic级别的延迟时间
     * 
     * @param topic 主题名称
     * @param delayMs 延迟时间（毫秒）
     * @throws IllegalArgumentException 如果该topic已在topic-partition配置中存在
     */
    public void setTopicDelay(String topic, long delayMs) {
        validateTopicNotInPartitionConfig(topic);
        topicDelayConfig.put(topic, delayMs);
        log.info("Set topic delay for '{}': {} ms", topic, delayMs);
    }
    
    /**
     * 设置topic-partition级别的延迟时间
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @param delayMs 延迟时间（毫秒）
     * @throws IllegalArgumentException 如果该topic已在topic配置中存在
     */
    public void setTopicPartitionDelay(String topic, int partition, long delayMs) {
        validateTopicNotInTopicConfig(topic);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        topicPartitionDelayConfig.put(topicPartition, delayMs);
        log.info("Set topic-partition delay for '{}': {} ms", topicPartition, delayMs);
    }
    
    /**
     * 批量设置topic级别的延迟配置
     * 
     * @param topicDelayMap topic和延迟时间的映射
     * @throws IllegalArgumentException 如果任何topic已在topic-partition配置中存在
     */
    public void setTopicDelays(Map<String, Long> topicDelayMap) {
        if (topicDelayMap != null) {
            // 先验证所有topic，避免部分设置成功后失败
            for (String topic : topicDelayMap.keySet()) {
                validateTopicNotInPartitionConfig(topic);
            }
            topicDelayConfig.putAll(topicDelayMap);
            log.info("Set topic delays: {}", topicDelayMap);
        }
    }
    
    /**
     * 批量设置topic-partition级别的延迟配置
     * 
     * @param topicPartitionDelayMap topic-partition和延迟时间的映射
     * @throws IllegalArgumentException 如果任何topic已在topic配置中存在
     */
    public void setTopicPartitionDelays(Map<TopicPartition, Long> topicPartitionDelayMap) {
        if (topicPartitionDelayMap != null) {
            // 先验证所有topic，避免部分设置成功后失败
            for (TopicPartition tp : topicPartitionDelayMap.keySet()) {
                validateTopicNotInTopicConfig(tp.topic());
            }
            topicPartitionDelayConfig.putAll(topicPartitionDelayMap);
            log.info("Set topic-partition delays: {}", topicPartitionDelayMap);
        }
    }
    
    /**
     * 获取指定topic和partition的延迟时间
     * 优先级：topic-partition配置 > topic配置
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @return 延迟时间（毫秒），如果没有配置则返回null
     */
    public Long getDelay(String topic, int partition) {
        // 优先查找topic-partition级别的配置
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long topicPartitionDelay = topicPartitionDelayConfig.get(topicPartition);
        if (topicPartitionDelay != null) {
            return topicPartitionDelay;
        }
        
        // 如果没有topic-partition级别的配置，则查找topic级别的配置
        return topicDelayConfig.get(topic);
    }
    
    /**
     * 获取指定topic的延迟时间（仅查找topic级别配置）
     * 
     * @param topic 主题名称
     * @return 延迟时间（毫秒），如果没有配置则返回null
     */
    public Long getTopicDelay(String topic) {
        return topicDelayConfig.get(topic);
    }
    
    /**
     * 获取指定topic-partition的延迟时间（仅查找topic-partition级别配置）
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @return 延迟时间（毫秒），如果没有配置则返回null
     */
    public Long getTopicPartitionDelay(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return topicPartitionDelayConfig.get(topicPartition);
    }
    
    /**
     * 移除topic级别的延迟配置
     * 
     * @param topic 主题名称
     * @return 被移除的延迟时间，如果没有配置则返回null
     */
    public Long removeTopicDelay(String topic) {
        Long removed = topicDelayConfig.remove(topic);
        if (removed != null) {
            log.info("Removed topic delay for '{}'", topic);
        }
        return removed;
    }
    
    /**
     * 移除topic-partition级别的延迟配置
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @return 被移除的延迟时间，如果没有配置则返回null
     */
    public Long removeTopicPartitionDelay(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long removed = topicPartitionDelayConfig.remove(topicPartition);
        if (removed != null) {
            log.info("Removed topic-partition delay for '{}'", topicPartition);
        }
        return removed;
    }
    
    /**
     * 获取所有topic级别的延迟配置
     * 
     * @return topic延迟配置的副本
     */
    public Map<String, Long> getAllTopicDelays() {
        return new ConcurrentHashMap<>(topicDelayConfig);
    }
    
    /**
     * 获取所有topic-partition级别的延迟配置
     * 
     * @return topic-partition延迟配置的副本
     */
    public Map<TopicPartition, Long> getAllTopicPartitionDelays() {
        return new ConcurrentHashMap<>(topicPartitionDelayConfig);
    }
    
    /**
     * 清空所有延迟配置
     */
    public void clear() {
        topicDelayConfig.clear();
        topicPartitionDelayConfig.clear();
        log.info("Cleared all delay configurations");
    }
    
    /**
     * 验证topic配置的互斥性
     * 
     * @throws IllegalArgumentException 如果存在重复的topic
     */
    private void validateTopicMutualExclusion() {
        Set<String> topicConfigTopics = topicDelayConfig.keySet();
        Set<String> partitionConfigTopics = new HashSet<>();
        
        for (TopicPartition tp : topicPartitionDelayConfig.keySet()) {
            partitionConfigTopics.add(tp.topic());
        }
        
        Set<String> intersection = new HashSet<>(topicConfigTopics);
        intersection.retainAll(partitionConfigTopics);
        
        if (!intersection.isEmpty()) {
            throw new IllegalArgumentException(
                "Topic配置冲突：以下topic同时存在于topicDelayConfig和topicPartitionDelayConfig中: " + intersection);
        }
    }
    
    /**
     * 验证topic不在partition配置中
     * 
     * @param topic 要验证的topic
     * @throws IllegalArgumentException 如果topic已在partition配置中存在
     */
    private void validateTopicNotInPartitionConfig(String topic) {
        for (TopicPartition tp : topicPartitionDelayConfig.keySet()) {
            if (tp.topic().equals(topic)) {
                throw new IllegalArgumentException(
                    "Topic配置冲突：topic '" + topic + "' 已在topicPartitionDelayConfig中存在");
            }
        }
    }
    
    /**
     * 验证topic不在topic配置中
     * 
     * @param topic 要验证的topic
     * @throws IllegalArgumentException 如果topic已在topic配置中存在
     */
    private void validateTopicNotInTopicConfig(String topic) {
        if (topicDelayConfig.containsKey(topic)) {
            throw new IllegalArgumentException(
                "Topic配置冲突：topic '" + topic + "' 已在topicDelayConfig中存在");
        }
    }
}