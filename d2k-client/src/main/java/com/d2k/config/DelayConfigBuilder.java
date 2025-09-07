/**
 * @author xiajuan96
 */
package com.d2k.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DelayConfig的Builder类，支持链式调用和灵活的构造能力
 */
public class DelayConfigBuilder {
    private static final Logger log = LoggerFactory.getLogger(DelayConfigBuilder.class);
    
    // topic级别的延迟配置
    private final Map<String, Long> topicDelayConfig;
    
    // topic-partition级别的延迟配置
    private final Map<TopicPartition, Long> topicPartitionDelayConfig;
    
    // Kafka集群的bootstrap servers，用于验证topic和partition的合法性
    private final String bootstrapServers;
    
    /**
     * 创建DelayConfigBuilder实例
     */
    public DelayConfigBuilder() {
        this.topicDelayConfig = new HashMap<>();
        this.topicPartitionDelayConfig = new HashMap<>();
        this.bootstrapServers = null;
        log.debug("Created DelayConfigBuilder instance without bootstrap servers");
    }
    
    /**
     * 创建DelayConfigBuilder实例，指定bootstrap servers用于验证
     * 
     * @param bootstrapServers Kafka集群的bootstrap servers
     */
    public DelayConfigBuilder(String bootstrapServers) {
        this.topicDelayConfig = new HashMap<>();
        this.topicPartitionDelayConfig = new HashMap<>();
        this.bootstrapServers = bootstrapServers;
        log.debug("Created DelayConfigBuilder instance with bootstrap servers: {}", bootstrapServers);
    }
    
    /**
     * 设置topic级别的延迟时间
     * 
     * @param topic 主题名称
     * @param delayMs 延迟时间（毫秒）
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数无效
     */
    public DelayConfigBuilder withTopicDelay(String topic, long delayMs) {
        validateTopic(topic);
        validateDelayMs(delayMs);
        validateTopicNotInPartitionConfig(topic);
        
        topicDelayConfig.put(topic, delayMs);
        log.debug("Added topic delay: {} -> {} ms", topic, delayMs);
        return this;
    }
    
    /**
     * 设置topic-partition级别的延迟时间
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @param delayMs 延迟时间（毫秒）
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数无效
     */
    public DelayConfigBuilder withTopicPartitionDelay(String topic, int partition, long delayMs) {
        validateTopic(topic);
        validatePartition(partition);
        validateDelayMs(delayMs);
        validateTopicNotInTopicConfig(topic);
        
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        topicPartitionDelayConfig.put(topicPartition, delayMs);
        log.debug("Added topic-partition delay: {} -> {} ms", topicPartition, delayMs);
        return this;
    }
    
    /**
     * 批量设置topic级别的延迟配置
     * 
     * @param topicDelayMap topic和延迟时间的映射
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数无效
     */
    public DelayConfigBuilder withTopicDelays(Map<String, Long> topicDelayMap) {
        if (topicDelayMap == null) {
            throw new IllegalArgumentException("Topic delay map cannot be null");
        }
        
        // 验证所有条目
        for (Map.Entry<String, Long> entry : topicDelayMap.entrySet()) {
            validateTopic(entry.getKey());
            validateDelayMs(entry.getValue());
            validateTopicNotInPartitionConfig(entry.getKey());
        }
        
        this.topicDelayConfig.putAll(topicDelayMap);
        log.debug("Added topic delays: {}", topicDelayMap);
        return this;
    }
    
    /**
     * 批量设置topic-partition级别的延迟配置
     * 
     * @param topicPartitionDelayMap topic-partition和延迟时间的映射
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数无效
     */
    public DelayConfigBuilder withTopicPartitionDelays(Map<TopicPartition, Long> topicPartitionDelayMap) {
        if (topicPartitionDelayMap == null) {
            throw new IllegalArgumentException("Topic-partition delay map cannot be null");
        }
        
        // 验证所有条目
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionDelayMap.entrySet()) {
            validateTopic(entry.getKey().topic());
            validatePartition(entry.getKey().partition());
            validateDelayMs(entry.getValue());
            validateTopicNotInTopicConfig(entry.getKey().topic());
        }
        
        this.topicPartitionDelayConfig.putAll(topicPartitionDelayMap);
        log.debug("Added topic-partition delays: {}", topicPartitionDelayMap);
        return this;
    }
    
    /**
     * 从现有的DelayConfig复制配置
     * 
     * @param delayConfig 现有的DelayConfig实例
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数为null
     */
    public DelayConfigBuilder copyFrom(DelayConfig delayConfig) {
        if (delayConfig == null) {
            throw new IllegalArgumentException("DelayConfig cannot be null");
        }
        
        Map<String, Long> existingTopicDelays = delayConfig.getAllTopicDelays();
        Map<TopicPartition, Long> existingTopicPartitionDelays = delayConfig.getAllTopicPartitionDelays();
        
        if (existingTopicDelays != null && !existingTopicDelays.isEmpty()) {
            this.topicDelayConfig.putAll(existingTopicDelays);
        }
        
        if (existingTopicPartitionDelays != null && !existingTopicPartitionDelays.isEmpty()) {
            this.topicPartitionDelayConfig.putAll(existingTopicPartitionDelays);
        }
        
        log.debug("Copied configuration from existing DelayConfig");
        return this;
    }
    
    /**
     * 清空所有配置
     * 
     * @return DelayConfigBuilder实例，支持链式调用
     */
    public DelayConfigBuilder clear() {
        topicDelayConfig.clear();
        topicPartitionDelayConfig.clear();
        log.debug("Cleared all configurations");
        return this;
    }
    
    /**
     * 清空topic级别的配置
     * 
     * @return DelayConfigBuilder实例，支持链式调用
     */
    public DelayConfigBuilder clearTopicDelays() {
        topicDelayConfig.clear();
        log.debug("Cleared topic delay configurations");
        return this;
    }
    
    /**
     * 清空topic-partition级别的配置
     * 
     * @return DelayConfigBuilder实例，支持链式调用
     */
    public DelayConfigBuilder clearTopicPartitionDelays() {
        topicPartitionDelayConfig.clear();
        log.debug("Cleared topic-partition delay configurations");
        return this;
    }
    
    /**
     * 移除指定topic的延迟配置
     * 
     * @param topic 主题名称
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果topic无效
     */
    public DelayConfigBuilder removeTopicDelay(String topic) {
        validateTopic(topic);
        topicDelayConfig.remove(topic);
        log.debug("Removed topic delay for: {}", topic);
        return this;
    }
    
    /**
     * 移除指定topic-partition的延迟配置
     * 
     * @param topic 主题名称
     * @param partition 分区号
     * @return DelayConfigBuilder实例，支持链式调用
     * @throws IllegalArgumentException 如果参数无效
     */
    public DelayConfigBuilder removeTopicPartitionDelay(String topic, int partition) {
        validateTopic(topic);
        validatePartition(partition);
        
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        topicPartitionDelayConfig.remove(topicPartition);
        log.debug("Removed topic-partition delay for: {}", topicPartition);
        return this;
    }
    
    /**
     * 检查是否有任何配置
     * 
     * @return 如果有任何配置则返回true，否则返回false
     */
    public boolean hasAnyConfiguration() {
        return !topicDelayConfig.isEmpty() || !topicPartitionDelayConfig.isEmpty();
    }
    
    /**
     * 获取当前topic级别配置的数量
     * 
     * @return topic配置数量
     */
    public int getTopicConfigCount() {
        return topicDelayConfig.size();
    }
    
    /**
     * 获取topic-partition配置数量
     * 
     * @return topic-partition配置数量
     */
    public int getTopicPartitionConfigCount() {
        return topicPartitionDelayConfig.size();
    }
    
    /**
     * 获取bootstrap servers配置
     * 
     * @return bootstrap servers字符串，如果未配置则返回null
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }
    
    /**
     * 构建DelayConfig实例
     * 如果配置了bootstrapServers，将连接Kafka集群验证topic配置的合理性
     * 
     * @return 构建完成的DelayConfig实例
     * @throws IllegalArgumentException 如果topic配置不合理
     */
    public DelayConfig build() {
        // 验证topic配置的互斥性
        validateTopicMutualExclusion();
        
        // 如果配置了bootstrapServers，进行Kafka集群验证
        if (bootstrapServers != null && !bootstrapServers.trim().isEmpty()) {
            validateWithKafkaCluster();
        }
        
        // 创建副本以确保线程安全
        Map<String, Long> topicDelayCopy = new ConcurrentHashMap<>(topicDelayConfig);
        Map<TopicPartition, Long> topicPartitionDelayCopy = new ConcurrentHashMap<>(topicPartitionDelayConfig);
        
        DelayConfig delayConfig = new DelayConfig(topicDelayCopy, topicPartitionDelayCopy);
        log.info("Built DelayConfig with {} topic delays and {} topic-partition delays", 
                topicDelayCopy.size(), topicPartitionDelayCopy.size());
        
        return delayConfig;
    }
    
    /**
     * 通过Kafka集群验证所有配置的topic和partition
     */
    private void validateWithKafkaCluster() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", "10000");
        props.put("connections.max.idle.ms", "10000");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            // 收集所有需要验证的topic
            Set<String> topicsToValidate = new HashSet<>();
            topicsToValidate.addAll(topicDelayConfig.keySet());
            
            for (TopicPartition tp : topicPartitionDelayConfig.keySet()) {
                topicsToValidate.add(tp.topic());
            }
            
            if (topicsToValidate.isEmpty()) {
                return;
            }
            
            // 获取topic描述信息
            DescribeTopicsResult describeResult = adminClient.describeTopics(topicsToValidate);
            Map<String, TopicDescription> topicDescriptions = describeResult.all().get(10, TimeUnit.SECONDS);
            
            // 验证topic-partition配置
            for (Map.Entry<TopicPartition, Long> entry : topicPartitionDelayConfig.entrySet()) {
                TopicPartition tp = entry.getKey();
                String topic = tp.topic();
                int partition = tp.partition();
                
                TopicDescription description = topicDescriptions.get(topic);
                if (description == null) {
                    throw new IllegalArgumentException("Topic does not exist: " + topic);
                }
                
                int partitionCount = description.partitions().size();
                if (partition >= partitionCount) {
                    throw new IllegalArgumentException(
                        String.format("Partition %d does not exist for topic %s (max partition: %d)", 
                                     partition, topic, partitionCount - 1));
                }
            }
            
            log.info("Successfully validated {} topics with Kafka cluster", topicsToValidate.size());
            
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new IllegalArgumentException("Failed to validate topics with Kafka cluster: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Validation interrupted", e);
        } catch (TimeoutException e) {
            throw new IllegalArgumentException("Timeout while validating topics with Kafka cluster", e);
        }
    }
    
    /**
     * 创建一个新的DelayConfigBuilder实例
     * 
     * @return 新的DelayConfigBuilder实例
     */
    public static DelayConfigBuilder newBuilder() {
        return new DelayConfigBuilder();
    }
    
    /**
     * 从现有DelayConfig创建Builder实例
     * 
     * @param delayConfig 现有的DelayConfig实例
     * @return 包含现有配置的DelayConfigBuilder实例
     * @throws IllegalArgumentException 如果delayConfig为null
     */
    public static DelayConfigBuilder fromDelayConfig(DelayConfig delayConfig) {
        return new DelayConfigBuilder().copyFrom(delayConfig);
    }
    
    // 私有验证方法
    
    /**
     * 验证topic名称
     */
    private void validateTopic(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (topic.length() > 249) {
            throw new IllegalArgumentException("Topic name cannot exceed 249 characters");
        }
        
        // 检查null字符
        if (topic.contains("\0")) {
            throw new IllegalArgumentException("Topic name cannot contain null character");
        }
        
        // 检查特殊名称
        if (".".equals(topic) || "..".equals(topic)) {
            throw new IllegalArgumentException("Topic name cannot be '.' or '..'");
        }
    }
    
    /**
     * 验证分区号
     */
    private void validatePartition(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be non-negative, got: " + partition);
        }
    }
    
    /**
     * 验证延迟时间
     */
    private void validateDelayMs(Long delayMs) {
        if (delayMs == null) {
            throw new IllegalArgumentException("Delay time cannot be null");
        }
        if (delayMs < 0) {
            throw new IllegalArgumentException("Delay time must be non-negative, got: " + delayMs);
        }
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
    
    @Override
    public String toString() {
        return "DelayConfigBuilder{" +
                "topicDelayConfig=" + topicDelayConfig +
                ", topicPartitionDelayConfig=" + topicPartitionDelayConfig +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                '}';
    }
}