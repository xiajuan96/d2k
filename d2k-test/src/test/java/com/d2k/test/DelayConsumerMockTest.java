package com.d2k.test;

import com.d2k.consumer.DelayConsumerRunnable;
import com.d2k.consumer.DelayItem;
import com.d2k.consumer.DelayItemHandler;
import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import org.apache.kafka.common.serialization.StringSerializer;

public class DelayConsumerMockTest {
    Logger log = LoggerFactory.getLogger(DelayConsumerMockTest.class);

    @Test
    public void testDelayDeliveryWithMockConsumer() throws Exception {
        String topic = "d2k-demo";
        TopicPartition tp0 = new TopicPartition(topic, 0);

        // Prepare MockConsumer
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.subscribe(Collections.singletonList(topic));

        // Assign and set beginning offsets
        mockConsumer.rebalance(Collections.singletonList(tp0));
        Map<TopicPartition, Long> begin = new HashMap<>();
        begin.put(tp0, 0L);
        mockConsumer.updateBeginningOffsets(begin);

        // Prepare records with delay headers (deliver after ~300ms)
        long baseTs = System.currentTimeMillis();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>(topic, 0, 0L, baseTs, TimestampType.CREATE_TIME,
                0L, 0, 0, "k1", "v1",
                new RecordHeadersBuilder().add("d2k-delay-ms", "300").build()));
        records.add(new ConsumerRecord<>(topic, 0, 1L, baseTs, TimestampType.CREATE_TIME,
                0L, 0, 0, "k2", "v2",
                new RecordHeadersBuilder().add("d2k-delay-ms", "100").build()));

        mockConsumer.addRecord(records.get(0));
        mockConsumer.addRecord(records.get(1));

        // Capture processed order and times
        ConcurrentLinkedQueue<String> processed = new ConcurrentLinkedQueue<>();
        DelayItemHandler<String, String> handler = new DelayItemHandler<String, String>() {
            @Override
            public void process(DelayItem<String, String> delayItem) {
                log.info("start process delayItem: {}",delayItem);
                processed.add(delayItem.getRecord().key() + ":" + delayItem.getRecord().value());
            }
        };

        // Start runnable with injected MockConsumer
        DelayConsumerRunnable<String, String> runnable = new DelayConsumerRunnable<>(
                mockConsumer,
                Collections.singletonList(topic),
                handler
        );

        Thread t = new Thread(runnable);
        t.start();

        // allow some time for processing
        Thread.sleep(700L);

        runnable.shutdown();
        t.join(1000L);

        // Expect both processed in order of due time: k2 first (100ms), then k1 (300ms)
        List<String> result = new ArrayList<>(processed);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("k2:v2", result.get(0));
        Assert.assertEquals("k1:v1", result.get(1));
    }

    @Test
    public void testProducerHeadersWithMockProducer() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, null, null);
        String topic = "d2k-demo";

        // create a record and add headers manually (simulating DelayProducer behavior)
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k", "v");
        record.headers().add(new RecordHeader("d2k-delay-ms", "500".getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("d2k-deliver-at", "123456".getBytes(StandardCharsets.UTF_8)));

        mockProducer.send(record);

        Assert.assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, String> sent = mockProducer.history().get(0);
        Assert.assertNotNull(sent.headers().lastHeader("d2k-delay-ms"));
        Assert.assertNotNull(sent.headers().lastHeader("d2k-deliver-at"));
        mockProducer.close();
    }

    // small helper to build headers for ConsumerRecord since its ctor takes Headers
    static class RecordHeadersBuilder {
        private final org.apache.kafka.common.header.internals.RecordHeaders headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        RecordHeadersBuilder add(String k, String v) {
            headers.add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
            return this;
        }
        org.apache.kafka.common.header.Headers build() { return headers; }
    }
    
    @Test
    public void testDelayProducerWithMockProducer() throws Exception {
        // 创建MockProducer
        MockProducer<String, String> mockProducer = new MockProducer<>(true, 
                new StringSerializer(), new StringSerializer());
        
        // 使用MockProducer创建DelayProducer
        DelayProducer<String, String> delayProducer = new DelayProducer<>(mockProducer);
        
        String topic = "d2k-delay-test";
        
        // 使用DelayProducer发送延迟消息
        Future<RecordMetadata> future1 = delayProducer.sendWithDelay(topic, "key1", "value1", 1000);
        Future<RecordMetadata> future2 = delayProducer.sendWithDelay(topic, "key2", "value2", 500);
        Future<RecordMetadata> future3 = delayProducer.sendDeliverAt(topic, "key3", "value3", 
                System.currentTimeMillis() + 1500);
        
        // 验证消息已发送
        Assert.assertEquals(3, mockProducer.history().size());
        
        // 验证消息头部
        ProducerRecord<String, String> record1 = mockProducer.history().get(0);
        ProducerRecord<String, String> record2 = mockProducer.history().get(1);
        ProducerRecord<String, String> record3 = mockProducer.history().get(2);
        
        Assert.assertNotNull(record1.headers().lastHeader("d2k-delay-ms"));
        Assert.assertNotNull(record2.headers().lastHeader("d2k-delay-ms"));
        Assert.assertNotNull(record3.headers().lastHeader("d2k-deliver-at"));
        
        // 验证延迟时间
        String delayMs1 = new String(record1.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        String delayMs2 = new String(record2.headers().lastHeader("d2k-delay-ms").value(), StandardCharsets.UTF_8);
        String deliverAt = new String(record3.headers().lastHeader("d2k-deliver-at").value(), StandardCharsets.UTF_8);
        
        Assert.assertEquals("1000", delayMs1);
        Assert.assertEquals("500", delayMs2);
        
        // 关闭资源
        delayProducer.close();
    }
}


