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
package com.d2k.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 延迟消息发送回调接口
 * 
 * @author xiajuan96
 * @since 1.0.2
 */
public interface DelayCallback {
    
    /**
     * 消息发送成功时的回调
     * 
     * @param metadata 消息元数据
     */
    void onSuccess(RecordMetadata metadata);
    
    /**
     * 消息发送失败时的回调
     * 
     * @param exception 异常信息
     */
    void onFailure(Exception exception);
}