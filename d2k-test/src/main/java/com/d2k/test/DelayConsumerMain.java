package com.d2k.test;

import com.d2k.consumer.DefaultLogDelayItemHandler;
import com.d2k.consumer.DelayConsumerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class DelayConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(DelayConsumerMain.class);

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "localhost:9092";
        List<String> topics = Arrays.asList("test-topic");

        DelayConsumerContainer<String, String> container = DelayConsumerContainer.simpleContainer(bootstrapServers,
                topics, new DefaultLogDelayItemHandler<>());
        container.start();

        while (true) {
            Thread.sleep(1000L);
        }

    }
}
