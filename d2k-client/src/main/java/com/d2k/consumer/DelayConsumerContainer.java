/*
 * D2K - Delay to Kafka
 * Copyright (C) 2024 xiajuan96
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package com.d2k.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xiajuan96
 * @date 2025/8/8 16:37
 */
public class DelayConsumerContainer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DelayConsumerContainer.class);
    
    private final int concurrency;
    private final Map<String, Object> configs;
    private final Collection<String> topics;
    private final DelayItemHandler<K, V> delayItemHandler;
    private final AsyncProcessingConfig asyncProcessingConfig;
    private final List<DelayConsumerRunnable<K, V>> workers = new ArrayList<>();
    private ExecutorService executor;

    public DelayConsumerContainer(int concurrency,
                                  Map<String, Object> configs,
                                  Collection<String> topics,
                                  DelayItemHandler<K, V> delayItemHandler,
                                  AsyncProcessingConfig asyncProcessingConfig) {
        this.concurrency = Math.max(1, concurrency);
        this.configs = configs;
        this.topics = topics;
        this.delayItemHandler = delayItemHandler;
        this.asyncProcessingConfig = asyncProcessingConfig != null ? asyncProcessingConfig : AsyncProcessingConfig.createSyncConfig();
    }
    
    /**
     * 兼容性构造函数（默认同步模式）
     */
    public DelayConsumerContainer(int concurrency,
                                  Map<String, Object> configs,
                                  Collection<String> topics,
                                  DelayItemHandler<K, V> delayItemHandler) {
        this(concurrency, configs, topics, delayItemHandler, AsyncProcessingConfig.createSyncConfig());
    }

    public void start() {
        log.info("Starting DelayConsumerContainer with concurrency: {} for topics: {}", concurrency, topics);
        if (executor != null) {
            log.warn("DelayConsumerContainer already started, ignoring start request");
            return;
        }
        executor = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            DelayConsumerRunnable<K, V> r = new DelayConsumerRunnable<>(configs,
                    topics, delayItemHandler, asyncProcessingConfig);
            workers.add(r);
            executor.submit(r);
        }
    }

    public void stop() {
        log.info("Stopping DelayConsumerContainer");
        if (executor == null) {
            log.warn("DelayConsumerContainer not running, ignoring stop request");
            return;
        }
        for (DelayConsumerRunnable<K, V> worker : workers) {
            worker.shutdown();
        }
        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            if (!terminated) {
                log.warn("Not all workers terminated within timeout period");
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for workers to terminate");
            Thread.currentThread().interrupt();
        } finally {
            executor = null;
            workers.clear();
            log.info("DelayConsumerContainer stopped");
        }
    }



}
