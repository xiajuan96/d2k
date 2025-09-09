package com.d2k.consumer;

import com.d2k.utils.ConfigUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * D2K消费者配置类
 * 封装所有D2K专有配置参数，提供类型安全的配置访问和验证
 * 
 * @author xiajuan96
 */
public class D2kConsumerConfig {
    
    // 配置键常量
    public static final String LOOP_TOTAL_MS_CONFIG = "d2k.loop.total.ms";
    public static final String QUEUE_CAPACITY_CONFIG = "d2k.queue.capacity";
    
    // 默认值常量
    public static final long DEFAULT_LOOP_TOTAL_MS = 200L;
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;
    
    // 配置属性
    private final long loopTotalMs;
    private final int queueCapacity;
    private final Map<String, Object> originalConfigs;
    
    /**
     * 构造函数
     * 
     * @param configs 原始配置Map
     */
    public D2kConsumerConfig(Map<String, Object> configs) {
        this.originalConfigs = new HashMap<>(configs);
        this.loopTotalMs = ConfigUtils.getLongConfig(configs, LOOP_TOTAL_MS_CONFIG, DEFAULT_LOOP_TOTAL_MS);
        this.queueCapacity = ConfigUtils.getIntConfig(configs, QUEUE_CAPACITY_CONFIG, DEFAULT_QUEUE_CAPACITY);
        
        // 验证配置
        validateConfigs();
    }
    
    /**
     * 默认构造函数，使用默认配置
     */
    public D2kConsumerConfig() {
        this.originalConfigs = new HashMap<>();
        this.loopTotalMs = DEFAULT_LOOP_TOTAL_MS;
        this.queueCapacity = DEFAULT_QUEUE_CAPACITY;
    }
    
    /**
     * 获取循环总时间（毫秒）
     * 
     * @return 循环总时间
     */
    public long getLoopTotalMs() {
        return loopTotalMs;
    }
    
    /**
     * 获取队列容量阈值
     * 
     * @return 队列容量阈值
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }
    
    /**
     * 获取原始配置Map的只读副本
     * 
     * @return 原始配置Map
     */
    public Map<String, Object> getOriginalConfigs() {
        return new HashMap<>(originalConfigs);
    }
    
    /**
     * 从配置Map中提取D2K专有配置
     * 
     * @param allConfigs 包含所有配置的Map
     * @return D2K专有配置Map
     */
    public static Map<String, Object> extractD2kConfigs(Map<String, Object> allConfigs) {
        Map<String, Object> d2kConfigs = new HashMap<>();
        for (Map.Entry<String, Object> entry : allConfigs.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("d2k.")) {
                d2kConfigs.put(key, entry.getValue());
            }
        }
        return d2kConfigs;
    }
    
    /**
     * 验证配置参数的有效性
     */
    private void validateConfigs() {
        if (loopTotalMs <= 0) {
            throw new IllegalArgumentException("Loop total milliseconds must be positive, got: " + loopTotalMs);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be positive, got: " + queueCapacity);
        }
    }
    

    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        D2kConsumerConfig that = (D2kConsumerConfig) o;
        return loopTotalMs == that.loopTotalMs &&
               queueCapacity == that.queueCapacity &&
               Objects.equals(originalConfigs, that.originalConfigs);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(loopTotalMs, queueCapacity, originalConfigs);
    }
    
    @Override
    public String toString() {
        return "D2kConsumerConfig{" +
               "loopTotalMs=" + loopTotalMs +
               ", queueCapacity=" + queueCapacity +
               ", originalConfigs=" + originalConfigs +
               '}';
    }
}