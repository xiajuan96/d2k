package com.d2k.test;

import com.d2k.producer.DelayProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DelayProducerMain {
    static final Logger log = LoggerFactory.getLogger(DelayProducerMain.class);
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String topic = "test-topic";
        String bootstrapServers = "localhost:9092";
        DelayProducer<String, String> producer = DelayProducer.buildStringProducer(bootstrapServers);

        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> future = producer.sendWithDelay(topic, "test-delay-" + i, 5000L);
            RecordMetadata recordMetadata = future.get();
            log.info("topic-partition:{}, offset: {}",recordMetadata.toString(),recordMetadata.offset());
            Thread.sleep(100L);
        }
        producer.close();
    }
}
