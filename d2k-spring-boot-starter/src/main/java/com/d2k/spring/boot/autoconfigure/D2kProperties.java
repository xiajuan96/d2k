package com.d2k.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * D2K 配置属性类
 * 用于配置 D2K 延迟消息系统的相关参数
 */
@ConfigurationProperties(prefix = "d2k")
public class D2kProperties {

    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();

    public Producer getProducer() {
        return producer;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    /**
     * 生产者配置
     */
    public static class Producer {
        private String bootstrapServers = "localhost:9092";
        private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
        private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
        private String clientId = "";
        private int retries = 3;
        private int batchSize = 16384;
        private long bufferMemory = 33554432L;
        private Map<String, Long> topicDelays = new HashMap<>();

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getKeySerializer() {
            return keySerializer;
        }

        public void setKeySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
        }

        public String getValueSerializer() {
            return valueSerializer;
        }

        public void setValueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getBufferMemory() {
            return bufferMemory;
        }

        public void setBufferMemory(long bufferMemory) {
            this.bufferMemory = bufferMemory;
        }

        public Map<String, Long> getTopicDelays() {
            return topicDelays;
        }

        public void setTopicDelays(Map<String, Long> topicDelays) {
            this.topicDelays = topicDelays;
        }
    }

    /**
     * 消费者配置
     */
    public static class Consumer {
        private String bootstrapServers = "localhost:9092";
        private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        private String groupId = "d2k-consumer-group";
        private String clientId = "d2k-consumer";
        private boolean enableAutoCommit = true;
        private int sessionTimeoutMs = 30000;
        private int heartbeatIntervalMs = 3000;
        private int maxPollRecords = 500;
        private int fetchMaxWaitMs = 500;
        private int concurrency = 1;
        private Map<String, Long> topicDelays = new HashMap<>();
        
        // 异步处理配置
        private boolean asyncProcessingEnabled = false;
        private int asyncCorePoolSize = 2;
        private int asyncMaximumPoolSize = 4;
        private long asyncKeepAliveTime = 60L;
        private int asyncQueueCapacity = 100;
        private String asyncRejectedExecutionPolicy = "CALLER_RUNS";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getKeyDeserializer() {
            return keyDeserializer;
        }

        public void setKeyDeserializer(String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public String getValueDeserializer() {
            return valueDeserializer;
        }

        public void setValueDeserializer(String valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public boolean getEnableAutoCommit() {
            return enableAutoCommit;
        }

        public void setEnableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }

        public int getSessionTimeoutMs() {
            return sessionTimeoutMs;
        }

        public void setSessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
        }

        public int getHeartbeatIntervalMs() {
            return heartbeatIntervalMs;
        }

        public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public int getMaxPollRecords() {
            return maxPollRecords;
        }

        public void setMaxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public int getFetchMaxWaitMs() {
            return fetchMaxWaitMs;
        }

        public void setFetchMaxWaitMs(int fetchMaxWaitMs) {
            this.fetchMaxWaitMs = fetchMaxWaitMs;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public Map<String, Long> getTopicDelays() {
            return topicDelays;
        }

        public void setTopicDelays(Map<String, Long> topicDelays) {
            this.topicDelays = topicDelays;
        }

        public boolean isAsyncProcessingEnabled() {
            return asyncProcessingEnabled;
        }

        public void setAsyncProcessingEnabled(boolean asyncProcessingEnabled) {
            this.asyncProcessingEnabled = asyncProcessingEnabled;
        }

        public int getAsyncCorePoolSize() {
            return asyncCorePoolSize;
        }

        public void setAsyncCorePoolSize(int asyncCorePoolSize) {
            this.asyncCorePoolSize = asyncCorePoolSize;
        }

        public int getAsyncMaximumPoolSize() {
            return asyncMaximumPoolSize;
        }

        public void setAsyncMaximumPoolSize(int asyncMaximumPoolSize) {
            this.asyncMaximumPoolSize = asyncMaximumPoolSize;
        }

        public long getAsyncKeepAliveTime() {
            return asyncKeepAliveTime;
        }

        public void setAsyncKeepAliveTime(long asyncKeepAliveTime) {
            this.asyncKeepAliveTime = asyncKeepAliveTime;
        }

        public int getAsyncQueueCapacity() {
            return asyncQueueCapacity;
        }

        public void setAsyncQueueCapacity(int asyncQueueCapacity) {
            this.asyncQueueCapacity = asyncQueueCapacity;
        }

        public String getAsyncRejectedExecutionPolicy() {
            return asyncRejectedExecutionPolicy;
        }

        public void setAsyncRejectedExecutionPolicy(String asyncRejectedExecutionPolicy) {
            this.asyncRejectedExecutionPolicy = asyncRejectedExecutionPolicy;
        }
    }
}