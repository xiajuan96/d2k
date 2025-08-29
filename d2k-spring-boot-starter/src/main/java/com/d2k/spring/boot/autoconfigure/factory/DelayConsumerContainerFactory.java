package com.d2k.spring.boot.autoconfigure.factory;

import com.d2k.consumer.AsyncProcessingConfig;
import com.d2k.consumer.DelayConsumerContainer;
import com.d2k.consumer.DelayItemHandler;
import com.d2k.spring.boot.autoconfigure.D2kProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * DelayConsumerContainer 工厂类
 * 用于在 Spring Boot 环境中创建和管理 DelayConsumerContainer 实例
 */
public class DelayConsumerContainerFactory {

    private final D2kProperties properties;

    public DelayConsumerContainerFactory(D2kProperties properties) {
        this.properties = properties;
    }

    /**
     * 创建 DelayConsumerContainer 实例
     *
     * @param topics           监听的主题
     * @param delayItemHandler 消息处理器
     * @param <K>              键类型
     * @param <V>              值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler) {
        return createContainer(topics, delayItemHandler,
                (Deserializer<K>) new StringDeserializer(),
                (Deserializer<V>) new StringDeserializer());
    }

    /**
     * 创建 DelayConsumerContainer 实例（指定并发数）
     *
     * @param topics           监听的主题
     * @param delayItemHandler 消息处理器
     * @param concurrency      并发消费者数量
     * @param <K>              键类型
     * @param <V>              值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler,
            int concurrency) {
        return createContainer(topics, delayItemHandler, concurrency,
                (Deserializer<K>) new StringDeserializer(),
                (Deserializer<V>) new StringDeserializer());
    }

    /**
     * 创建 DelayConsumerContainer 实例（指定并发数和异步配置）
     *
     * @param topics           监听的主题
     * @param delayItemHandler 消息处理器
     * @param concurrency      并发消费者数量
     * @param asyncConfig      异步处理配置
     * @param <K>              键类型
     * @param <V>              值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler,
            int concurrency,
            AsyncProcessingConfig asyncConfig) {
        return createContainer(topics, delayItemHandler, concurrency, asyncConfig,
                (Deserializer<K>) new StringDeserializer(),
                (Deserializer<V>) new StringDeserializer());
    }

    /**
     * 创建 DelayConsumerContainer 实例（指定序列化器）
     *
     * @param topics            监听的主题
     * @param delayItemHandler  消息处理器
     * @param keyDeserializer   键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param <K>               键类型
     * @param <V>               值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        return createContainer(topics, delayItemHandler,
                properties.getConsumer().getConcurrency(),
                keyDeserializer, valueDeserializer);
    }

    /**
     * 创建 DelayConsumerContainer 实例（指定并发数和序列化器）
     *
     * @param topics            监听的主题
     * @param delayItemHandler  消息处理器
     * @param concurrency       并发消费者数量
     * @param keyDeserializer   键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param <K>               键类型
     * @param <V>               值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler,
            int concurrency,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        // 构建异步处理配置
        AsyncProcessingConfig asyncConfig = buildAsyncProcessingConfig();

        return createContainer(topics, delayItemHandler, concurrency, asyncConfig,
                keyDeserializer, valueDeserializer);
    }

    /**
     * 创建 DelayConsumerContainer 实例（完整参数版本）
     *
     * @param topics            监听的主题
     * @param delayItemHandler  消息处理器
     * @param concurrency       并发消费者数量
     * @param asyncConfig       异步处理配置
     * @param keyDeserializer   键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param <K>               键类型
     * @param <V>               值类型
     * @return DelayConsumerContainer 实例
     */
    @SuppressWarnings("unchecked")
    public <K, V> DelayConsumerContainer<K, V> createContainer(
            Collection<String> topics,
            DelayItemHandler<K, V> delayItemHandler,
            int concurrency,
            AsyncProcessingConfig asyncConfig,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        // 构建消费者配置
        Map<String, Object> configs = buildConsumerConfigs();

        return new DelayConsumerContainer<K, V>(
                concurrency,
                configs,
                keyDeserializer,
                valueDeserializer,
                topics,
                delayItemHandler,
                asyncConfig
        );
    }

    /**
     * 构建消费者配置
     */
    private Map<String, Object> buildConsumerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getConsumer().getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getConsumer().getKeyDeserializer());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getConsumer().getValueDeserializer());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, properties.getConsumer().getClientId());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getConsumer().getEnableAutoCommit());
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getConsumer().getSessionTimeoutMs());
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, properties.getConsumer().getHeartbeatIntervalMs());
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getConsumer().getMaxPollRecords());
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, properties.getConsumer().getFetchMaxWaitMs());
        return configs;
    }

    /**
     * 构建异步处理配置
     * 根据 Consumer 配置中的异步处理参数创建 AsyncProcessingConfig
     */
    private AsyncProcessingConfig buildAsyncProcessingConfig() {
        D2kProperties.Consumer consumerProps = properties.getConsumer();

        AsyncProcessingConfig config = new AsyncProcessingConfig();
        config.setEnabled(consumerProps.isAsyncProcessingEnabled());
        config.setCorePoolSize(consumerProps.getAsyncCorePoolSize());
        config.setMaximumPoolSize(consumerProps.getAsyncMaximumPoolSize());
        config.setKeepAliveTime(consumerProps.getAsyncKeepAliveTime());
        config.setQueueCapacity(consumerProps.getAsyncQueueCapacity());

        // 解析拒绝策略字符串
        try {
            AsyncProcessingConfig.RejectedExecutionPolicy policy =
                    AsyncProcessingConfig.RejectedExecutionPolicy.valueOf(
                            consumerProps.getAsyncRejectedExecutionPolicy().toUpperCase());
            config.setRejectedExecutionPolicy(policy);
        } catch (IllegalArgumentException e) {
            // 如果配置的策略无效，使用默认策略
            config.setRejectedExecutionPolicy(AsyncProcessingConfig.RejectedExecutionPolicy.CALLER_RUNS);
        }

        return config;
    }

}