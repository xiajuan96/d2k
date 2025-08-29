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
package com.d2k.spring.boot.autoconfigure.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * D2K 延迟消息监听器注解
 * 用于标记处理延迟消息的方法
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface D2kListener {

    /**
     * 监听的主题
     */
    String topic();

    /**
     * 消费者组 ID
     */
    String groupId() default "";

    /**
     * 客户端 ID
     */
    String clientId() default "";

    /**
     * 并发消费者数量
     */
    int concurrency() default 1;

    /**
     * 是否启用异步处理
     * 当启用时，每个分区的消息处理将在独立的线程池中异步执行
     */
    boolean asyncProcessing() default false;

    /**
     * 异步处理核心线程数
     * 仅在 asyncProcessing=true 时生效
     */
    int asyncCorePoolSize() default 2;

    /**
     * 异步处理最大线程数
     * 仅在 asyncProcessing=true 时生效
     */
    int asyncMaxPoolSize() default 4;

    /**
     * 异步处理队列容量
     * 仅在 asyncProcessing=true 时生效
     */
    int asyncQueueCapacity() default 100;

    /**
     * 是否自动启动
     */
    boolean autoStartup() default true;
}