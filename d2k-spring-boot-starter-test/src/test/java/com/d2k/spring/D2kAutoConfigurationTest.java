package com.d2k.spring;

import com.d2k.spring.boot.autoconfigure.D2kProperties;
import com.d2k.spring.boot.autoconfigure.config.D2kAutoConfiguration;
import com.d2k.spring.boot.autoconfigure.factory.DelayConsumerContainerFactory;
import com.d2k.spring.boot.autoconfigure.manager.D2kConsumerManager;
import com.d2k.spring.boot.autoconfigure.template.D2kTemplate;
import com.d2k.spring.boot.autoconfigure.annotation.D2kListenerAnnotationBeanPostProcessor;
import com.d2k.producer.DelayProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * D2K自动配置测试
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {D2kAutoConfiguration.class}, 
    properties = {
        "d2k.producer.bootstrap-servers=localhost:9092",
        "d2k.consumer.bootstrap-servers=localhost:9092"
    })
public class D2kAutoConfigurationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void testDelayProducerBeanExists() {
        assertTrue("DelayProducer bean should exist", 
                applicationContext.containsBean("delayProducer"));
        
        DelayProducer delayProducer = applicationContext.getBean(DelayProducer.class);
        assertNotNull("DelayProducer should not be null", delayProducer);
    }

    @Test
    public void testDelayConsumerContainerFactoryBeanExists() {
        assertTrue("DelayConsumerContainerFactory bean should exist", 
                applicationContext.containsBean("delayConsumerContainerFactory"));
        
        DelayConsumerContainerFactory factory = applicationContext.getBean(DelayConsumerContainerFactory.class);
        assertNotNull("DelayConsumerContainerFactory should not be null", factory);
    }

    @Test
    public void testD2kTemplateBeanExists() {
        assertTrue("D2kTemplate bean should exist", 
                applicationContext.containsBean("d2kTemplate"));
        
        D2kTemplate<Object, Object> d2kTemplate = applicationContext.getBean("d2kTemplate", D2kTemplate.class);
        assertNotNull("D2kTemplate should not be null", d2kTemplate);
    }

    @Test
    public void testStringD2kTemplateBeanExists() {
        assertTrue("StringD2kTemplate bean should exist", 
                applicationContext.containsBean("stringD2kTemplate"));
        
        Object stringD2kTemplate = applicationContext.getBean("stringD2kTemplate");
        assertNotNull("StringD2kTemplate should not be null", stringD2kTemplate);
    }

    @Test
    public void testD2kConsumerManagerBeanExists() {
        assertTrue("D2kConsumerManager bean should exist", 
                applicationContext.containsBean("d2kConsumerManager"));
        
        D2kConsumerManager manager = applicationContext.getBean(D2kConsumerManager.class);
        assertNotNull("D2kConsumerManager should not be null", manager);
    }

    @Test
    public void testD2kListenerAnnotationBeanPostProcessorExists() {
        assertTrue("D2kListenerAnnotationBeanPostProcessor bean should exist", 
                applicationContext.containsBean("d2kListenerAnnotationBeanPostProcessor"));
        
        D2kListenerAnnotationBeanPostProcessor processor = 
                applicationContext.getBean(D2kListenerAnnotationBeanPostProcessor.class);
        assertNotNull("D2kListenerAnnotationBeanPostProcessor should not be null", processor);
    }

    @Test
    public void testD2kPropertiesBeanExists() {
        assertTrue("D2kProperties bean should exist", 
                applicationContext.containsBean("d2k-com.d2k.spring.boot.autoconfigure.D2kProperties"));
        
        D2kProperties properties = applicationContext.getBean(D2kProperties.class);
        assertNotNull("D2kProperties should not be null", properties);
        
        // 测试默认配置
        assertEquals("Default bootstrap servers should be localhost:9092", 
                "localhost:9092", properties.getProducer().getBootstrapServers());
        assertEquals("Default consumer group should be d2k-spring-consumer-group", 
                "d2k-spring-consumer-group", properties.getConsumer().getGroupId());
    }
}