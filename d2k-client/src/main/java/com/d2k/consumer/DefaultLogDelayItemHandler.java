package com.d2k.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogDelayItemHandler<K, V> implements DelayItemHandler<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultLogDelayItemHandler.class);

    @Override
    public void process(DelayItem<K, V> delayItem) {
        long processTime = System.currentTimeMillis();
        long delta = delayItem.getResumeAtTimestamp() - processTime;
        log.info("Processed deltaMs : {},  delayInfo : {}, processTime: {}", delta, delayItem, processTime);
    }
}
