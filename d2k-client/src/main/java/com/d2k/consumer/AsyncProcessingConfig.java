package com.d2k.consumer;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionHandler;

/**
 * 异步处理配置类
 * 
 * 用于配置延迟消息的异步处理参数，包括是否启用异步处理、线程池参数等。
 * 
 * @author xiajuan96
 * @date 2025/8/26
 */
public class AsyncProcessingConfig {
    
    /** 是否启用异步处理，默认为false（串行模式） */
    private boolean enabled = false;
    
    /** 核心线程数，默认为2 */
    private int corePoolSize = 2;
    
    /** 最大线程数，默认为4 */
    private int maximumPoolSize = 4;
    
    /** 线程空闲时间（秒），默认为60秒 */
    private long keepAliveTime = 60L;
    
    /** 队列长度，默认为100 */
    private int queueCapacity = 100;
    
    /** 拒绝策略，默认为CallerRunsPolicy */
    private RejectedExecutionPolicy rejectedExecutionPolicy = RejectedExecutionPolicy.CALLER_RUNS;
    
    /**
     * 拒绝策略枚举
     */
    public enum RejectedExecutionPolicy {
        /** 调用者运行策略 */
        CALLER_RUNS,
        /** 抛出异常策略 */
        ABORT,
        /** 丢弃策略 */
        DISCARD,
        /** 丢弃最老任务策略 */
        DISCARD_OLDEST
    }
    
    /**
     * 默认构造函数
     */
    public AsyncProcessingConfig() {
    }
    
    /**
     * 构造函数
     * 
     * @param enabled 是否启用异步处理
     */
    public AsyncProcessingConfig(boolean enabled) {
        this.enabled = enabled;
    }
    
    /**
     * 构造函数
     * 
     * @param enabled 是否启用异步处理
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大线程数
     * @param queueCapacity 队列长度
     */
    public AsyncProcessingConfig(boolean enabled, int corePoolSize, int maximumPoolSize, int queueCapacity) {
        this.enabled = enabled;
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.queueCapacity = queueCapacity;
    }
    
    /**
     * 创建默认的同步配置
     * 
     * @return 同步配置实例
     */
    public static AsyncProcessingConfig createSyncConfig() {
        return new AsyncProcessingConfig(false);
    }
    
    /**
     * 创建默认的异步配置
     * 
     * @return 异步配置实例
     */
    public static AsyncProcessingConfig createAsyncConfig() {
        return new AsyncProcessingConfig(true);
    }
    
    /**
     * 创建自定义异步配置
     * 
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大线程数
     * @param queueCapacity 队列长度
     * @return 异步配置实例
     */
    public static AsyncProcessingConfig createAsyncConfig(int corePoolSize, int maximumPoolSize, int queueCapacity) {
        return new AsyncProcessingConfig(true, corePoolSize, maximumPoolSize, queueCapacity);
    }
    
    /**
     * 转换为ThreadPoolExecutor的拒绝策略
     * 
     * @return RejectedExecutionHandler实例
     */
    public RejectedExecutionHandler toRejectedExecutionHandler() {
        switch (rejectedExecutionPolicy) {
            case CALLER_RUNS:
                return new ThreadPoolExecutor.CallerRunsPolicy();
            case ABORT:
                return new ThreadPoolExecutor.AbortPolicy();
            case DISCARD:
                return new ThreadPoolExecutor.DiscardPolicy();
            case DISCARD_OLDEST:
                return new ThreadPoolExecutor.DiscardOldestPolicy();
            default:
                return new ThreadPoolExecutor.CallerRunsPolicy();
        }
    }
    
    // Getter and Setter methods
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public int getCorePoolSize() {
        return corePoolSize;
    }
    
    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }
    
    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }
    
    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }
    
    public long getKeepAliveTime() {
        return keepAliveTime;
    }
    
    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }
    
    public int getQueueCapacity() {
        return queueCapacity;
    }
    
    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }
    
    public RejectedExecutionPolicy getRejectedExecutionPolicy() {
        return rejectedExecutionPolicy;
    }
    
    public void setRejectedExecutionPolicy(RejectedExecutionPolicy rejectedExecutionPolicy) {
        this.rejectedExecutionPolicy = rejectedExecutionPolicy;
    }
    
    @Override
    public String toString() {
        return "AsyncProcessingConfig{" +
                "enabled=" + enabled +
                ", corePoolSize=" + corePoolSize +
                ", maximumPoolSize=" + maximumPoolSize +
                ", keepAliveTime=" + keepAliveTime +
                ", queueCapacity=" + queueCapacity +
                ", rejectedExecutionPolicy=" + rejectedExecutionPolicy +
                '}';
    }
}