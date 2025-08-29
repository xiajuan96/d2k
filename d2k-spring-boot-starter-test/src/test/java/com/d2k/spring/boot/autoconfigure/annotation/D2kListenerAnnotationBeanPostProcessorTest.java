package com.d2k.spring.boot.autoconfigure.annotation;

import com.d2k.consumer.DelayItem;
import com.d2k.consumer.DelayItemHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

/**
 * D2kListener注解处理器测试
 * 验证多参数类型支持功能
 * 
 * @author xiajuan96
 */
@RunWith(MockitoJUnitRunner.class)
public class D2kListenerAnnotationBeanPostProcessorTest {

    private D2kListenerAnnotationBeanPostProcessor processor = new D2kListenerAnnotationBeanPostProcessor();

    /**
     * 测试消息处理器 - 包含各种参数类型的方法
     */
    public static class TestMessageHandler {
        
        public String lastStringMessage;
        public ConsumerRecord<?, ?> lastConsumerRecord;
        public Object lastObjectMessage;
        public Integer lastIntegerMessage;
        public boolean noParamMethodCalled;
        
        @D2kListener(topic = "test-topic", groupId = "test-group")
        public void handleStringMessage(String message) {
            this.lastStringMessage = message;
        }
        
        @D2kListener(topic = "test-topic", groupId = "test-group")
        public void handleConsumerRecord(ConsumerRecord<String, String> record) {
            this.lastConsumerRecord = record;
        }
        
        @D2kListener(topic = "test-topic", groupId = "test-group")
        public void handleObjectMessage(Object message) {
            this.lastObjectMessage = message;
        }
        
        @D2kListener(topic = "test-topic", groupId = "test-group")
        public void handleIntegerMessage(Integer number) {
            this.lastIntegerMessage = number;
        }
        
        @D2kListener(topic = "test-topic", groupId = "test-group")
        public void handleNoParamMessage() {
            this.noParamMethodCalled = true;
        }
    }

    @Test
    public void testStringParameterConversion() throws Exception {
        TestMessageHandler handler = new TestMessageHandler();
        Method method = TestMessageHandler.class.getMethod("handleStringMessage", String.class);
        
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(handler, method);
        DelayItem<Object, Object> delayItem = createTestDelayItem("Hello World");
        
        messageHandler.process(delayItem);
        
        assertEquals("Hello World", handler.lastStringMessage);
    }

    @Test
    public void testConsumerRecordParameterConversion() throws Exception {
        TestMessageHandler handler = new TestMessageHandler();
        Method method = TestMessageHandler.class.getMethod("handleConsumerRecord", ConsumerRecord.class);
        
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(handler, method);
        DelayItem<Object, Object> delayItem = createTestDelayItem("Test Message");
        
        messageHandler.process(delayItem);
        
        assertNotNull(handler.lastConsumerRecord);
        assertEquals("Test Message", handler.lastConsumerRecord.value());
        assertEquals("test-topic", handler.lastConsumerRecord.topic());
    }

    @Test
    public void testObjectParameterConversion() throws Exception {
        TestMessageHandler handler = new TestMessageHandler();
        Method method = TestMessageHandler.class.getMethod("handleObjectMessage", Object.class);
        
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(handler, method);
        DelayItem<Object, Object> delayItem = createTestDelayItem("Object Message");
        
        messageHandler.process(delayItem);
        
        assertEquals("Object Message", handler.lastObjectMessage);
    }

    @Test
    public void testIntegerParameterConversion() throws Exception {
        TestMessageHandler handler = new TestMessageHandler();
        Method method = TestMessageHandler.class.getMethod("handleIntegerMessage", Integer.class);
        
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(handler, method);
        DelayItem<Object, Object> delayItem = createTestDelayItem("123");
        
        messageHandler.process(delayItem);
        
        assertEquals(Integer.valueOf(123), handler.lastIntegerMessage);
    }

    @Test
    public void testNoParameterMethod() throws Exception {
        TestMessageHandler handler = new TestMessageHandler();
        Method method = TestMessageHandler.class.getMethod("handleNoParamMessage");
        
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(handler, method);
        DelayItem<Object, Object> delayItem = createTestDelayItem("Any Message");
        
        messageHandler.process(delayItem);
        
        assertTrue(handler.noParamMethodCalled);
    }

    @Test
    public void testValidateListenerMethodWithValidParameters() throws Exception {
        // 测试String参数
        Method stringMethod = TestMessageHandler.class.getMethod("handleStringMessage", String.class);
        // 应该不抛出异常
        validateListenerMethod(stringMethod);
        
        // 测试ConsumerRecord参数
        Method recordMethod = TestMessageHandler.class.getMethod("handleConsumerRecord", ConsumerRecord.class);
        // 应该不抛出异常
        validateListenerMethod(recordMethod);
        
        // 测试Object参数
        Method objectMethod = TestMessageHandler.class.getMethod("handleObjectMessage", Object.class);
        // 应该不抛出异常
        validateListenerMethod(objectMethod);
        
        // 测试无参数
        Method noParamMethod = TestMessageHandler.class.getMethod("handleNoParamMessage");
        // 应该不抛出异常
        validateListenerMethod(noParamMethod);
    }

    @Test
    public void testValidateListenerMethodWithTooManyParameters() throws Exception {
        // 创建一个有两个参数的方法用于测试
        class InvalidHandler {
            public void invalidMethod(String param1, String param2) {}
        }
        
        Method invalidMethod = InvalidHandler.class.getMethod("invalidMethod", String.class, String.class);
        
        try {
            validateListenerMethod(invalidMethod);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (Exception e) {
            // 反射调用会包装异常，需要检查原因
            Throwable cause = e.getCause();
            assertTrue("Expected IllegalArgumentException", cause instanceof IllegalArgumentException);
            assertTrue("Expected error message about parameter count", 
                      cause.getMessage().contains("@D2kListener method must have 0 or 1 parameter"));
        }
    }

    // 辅助方法：使用反射调用私有方法
    private DelayItemHandler<Object, Object> createMessageHandler(Object bean, Method method) throws Exception {
        Method createHandlerMethod = D2kListenerAnnotationBeanPostProcessor.class
                .getDeclaredMethod("createMessageHandler", Object.class, Method.class);
        createHandlerMethod.setAccessible(true);
        return (DelayItemHandler<Object, Object>) createHandlerMethod.invoke(processor, bean, method);
    }
    
    private void validateListenerMethod(Method method) throws Exception {
        Method validateMethod = D2kListenerAnnotationBeanPostProcessor.class
                .getDeclaredMethod("validateListenerMethod", Method.class);
        validateMethod.setAccessible(true);
        validateMethod.invoke(processor, method);
    }

    // 辅助方法：创建测试用的DelayItem
    private DelayItem<Object, Object> createTestDelayItem(Object value) {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "test-topic", 0, 0L, "test-key", value);
        
        long delayMilliseconds = 1000L;
        long resumeAtTimestamp = System.currentTimeMillis() + delayMilliseconds;
        
        return new DelayItem<>(delayMilliseconds, resumeAtTimestamp, record);
    }
}