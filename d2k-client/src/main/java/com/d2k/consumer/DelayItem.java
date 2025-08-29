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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 延迟消息项
 * 
 * @author xiajuan96
 * @date 2025/8/13 15:55
 */
public class DelayItem<K, V> implements Comparable<DelayItem<K, V>> {
    /**
     * 延迟时间 ms
     */
    private final long delayMilliseconds;
    /**
     * 恢复执行的时间戳
     */
    private final long resumeAtTimestamp;
    /**
     * record
     */
    private final ConsumerRecord<K, V> record;

    public DelayItem(long delayMilliseconds, long resumeAtTimestamp, ConsumerRecord<K, V> record) {
        this.delayMilliseconds = delayMilliseconds;
        this.resumeAtTimestamp = resumeAtTimestamp;
        this.record = record;
    }

    public long getDelayMilliseconds() {
        return delayMilliseconds;
    }

    public long getResumeAtTimestamp() {
        return resumeAtTimestamp;
    }

    public ConsumerRecord<K, V> getRecord() {
        return record;
    }

    @Override
    public int compareTo(DelayItem<K, V> o) {
        return Long.compare(resumeAtTimestamp,o.resumeAtTimestamp);
    }

    @Override
    public String toString() {
        return "DelayItem{" +
                "delayMilliseconds=" + delayMilliseconds +
                ", resumeAtTimestamp=" + resumeAtTimestamp +
                ", record=" + record +
                '}';
    }
}
