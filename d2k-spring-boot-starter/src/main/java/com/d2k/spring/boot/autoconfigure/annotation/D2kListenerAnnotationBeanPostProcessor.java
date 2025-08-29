package com.d2k.spring.boot.autoconfigure.annotation;

import com.d2k.consumer.DelayConsumerContainer;
import com.d2k.consumer.DelayItemHandler;
import com.d2k.consumer.DelayItem;
import com.d2k.consumer.AsyncProcessingConfig;
import com.d2k.spring.boot.autoconfigure.D2kProperties;
import com.d2k.spring.boot.autoconfigure.factory.DelayConsumerContainerFactory;
import com.d2k.spring.boot.autoconfigure.manager.D2kConsumerManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * D2K 监听器注解处理器
 * 处理 @D2kListener 注解，自动创建和配置消费者容器
 */
public class D2kListenerAnnotationBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

    private BeanFactory beanFactory;
    private D2kConsumerManager consumerManager;
    private D2kProperties d2kProperties;
    private DelayConsumerContainerFactory containerFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        this.consumerManager = beanFactory.getBean(D2kConsumerManager.class);
        this.d2kProperties = beanFactory.getBean(D2kProperties.class);
        this.containerFactory = beanFactory.getBean(DelayConsumerContainerFactory.class);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = bean.getClass();
        Method[] methods = targetClass.getDeclaredMethods();

        for (Method method : methods) {
            D2kListener annotation = AnnotationUtils.findAnnotation(method, D2kListener.class);
            if (annotation != null) {
                processD2kListener(bean, method, annotation);
            }
        }

        return bean;
    }

    private void processD2kListener(Object bean, Method method, D2kListener annotation) {
        // 验证方法签名
        validateListenerMethod(method);

        // 创建消息处理器
        DelayItemHandler<Object, Object> messageHandler = createMessageHandler(bean, method);

        // 使用工厂创建消费者容器，传递注解中的并发配置和异步处理配置
        java.util.Collection<String> topics = java.util.Collections.singletonList(annotation.topic());
        
        // 根据注解配置创建异步处理配置
        AsyncProcessingConfig asyncConfig = createAsyncProcessingConfig(annotation);
        
        DelayConsumerContainer<Object, Object> container = containerFactory.createContainer(
                topics, messageHandler, annotation.concurrency(), asyncConfig);

        // 生成容器名称并注册
        String containerName = generateContainerName(bean.getClass(), method, annotation);
        consumerManager.registerContainer(containerName, container);

        // 如果设置了自动启动，则启动容器
        if (annotation.autoStartup()) {
            container.start();
        }
    }

    private void validateListenerMethod(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        
        // 支持0个或1个参数
        if (parameterTypes.length > 1) {
            throw new IllegalArgumentException(
                "@D2kListener method must have 0 or 1 parameter, but found " + parameterTypes.length + ": " + method);
        }
        
        // 如果有参数，验证参数类型是否支持
        if (parameterTypes.length == 1) {
            Class<?> paramType = parameterTypes[0];
            
            // 支持的参数类型：String、ConsumerRecord、或任意Object类型（用于泛型支持）
            boolean isValidType = paramType == String.class || 
                                 ConsumerRecord.class.isAssignableFrom(paramType) ||
                                 paramType == Object.class;
            
            if (!isValidType) {
                // 对于其他类型，我们也允许，因为可能是泛型类型
                // 运行时会尝试进行类型转换
                // 这里只是记录一个警告日志
                System.out.println("Warning: @D2kListener method parameter type " + paramType.getName() + 
                                 " may require runtime type conversion: " + method);
            }
        }
    }

    private Properties buildConsumerProperties(D2kListener annotation) {
        Properties props = new Properties();
        
        // 基础配置
        props.put("bootstrap.servers", d2kProperties.getConsumer().getBootstrapServers());
        props.put("key.deserializer", d2kProperties.getConsumer().getKeyDeserializer());
        props.put("value.deserializer", d2kProperties.getConsumer().getValueDeserializer());
        props.put("enable.auto.commit", d2kProperties.getConsumer().getEnableAutoCommit());
        props.put("session.timeout.ms", d2kProperties.getConsumer().getSessionTimeoutMs());
        props.put("heartbeat.interval.ms", d2kProperties.getConsumer().getHeartbeatIntervalMs());
        props.put("max.poll.records", d2kProperties.getConsumer().getMaxPollRecords());
        props.put("fetch.max.wait.ms", d2kProperties.getConsumer().getFetchMaxWaitMs());
        
        // 注解特定配置
        String groupId = StringUtils.hasText(annotation.groupId()) ? 
            annotation.groupId() : d2kProperties.getConsumer().getGroupId();
        props.put("group.id", groupId);
        
        String clientId = StringUtils.hasText(annotation.clientId()) ? 
            annotation.clientId() : d2kProperties.getConsumer().getClientId();
        props.put("client.id", clientId);
        
        return props;
    }

    private DelayItemHandler<Object, Object> createMessageHandler(Object bean, Method method) {
        return new DelayItemHandler<Object, Object>() {
            @Override
            public void process(DelayItem<Object, Object> delayItem) {
                try {
                    method.setAccessible(true);
                    // 根据方法参数类型调用不同的方法
                    Class<?>[] paramTypes = method.getParameterTypes();
                    
                    if (paramTypes.length == 0) {
                        // 无参数方法
                        method.invoke(bean);
                    } else if (paramTypes.length == 1) {
                        Class<?> paramType = paramTypes[0];
                        Object argument = convertToParameterType(delayItem, paramType);
                        method.invoke(bean, argument);
                    } else {
                        throw new IllegalArgumentException("@D2kListener method must have 0 or 1 parameter");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error invoking @D2kListener method: " + method, e);
                }
            }
        };
    }
    
    /**
     * 将DelayItem转换为方法参数所需的类型
     */
    private Object convertToParameterType(DelayItem<Object, Object> delayItem, Class<?> paramType) {
        // 1. 如果参数类型是ConsumerRecord或其子类
        if (ConsumerRecord.class.isAssignableFrom(paramType)) {
            return delayItem.getRecord();
        }
        
        // 2. 如果参数类型是String
        if (paramType == String.class) {
            Object value = delayItem.getRecord().value();
            return value != null ? value.toString() : null;
        }
        
        // 3. 如果参数类型是Object（泛型擦除后的类型）
        if (paramType == Object.class) {
            return delayItem.getRecord().value();
        }
        
        // 4. 如果参数类型与record的value类型兼容
        Object recordValue = delayItem.getRecord().value();
        if (recordValue != null && paramType.isAssignableFrom(recordValue.getClass())) {
            return recordValue;
        }
        
        // 5. 尝试类型转换（适用于基本类型包装类等）
        if (recordValue != null) {
            try {
                // 对于数值类型的转换
                if (paramType == Integer.class || paramType == int.class) {
                    return Integer.valueOf(recordValue.toString());
                } else if (paramType == Long.class || paramType == long.class) {
                    return Long.valueOf(recordValue.toString());
                } else if (paramType == Double.class || paramType == double.class) {
                    return Double.valueOf(recordValue.toString());
                } else if (paramType == Boolean.class || paramType == boolean.class) {
                    return Boolean.valueOf(recordValue.toString());
                }
            } catch (NumberFormatException e) {
                 // 数值转换失败，继续使用原始值
             } catch (IllegalArgumentException e) {
                 // 其他转换失败，继续使用原始值
             }
        }
        
        // 6. 默认返回record的value，让运行时进行类型检查
        return recordValue;
    }

    /**
     * 根据注解配置创建异步处理配置
     * 
     * @param annotation D2kListener注解
     * @return AsyncProcessingConfig实例
     */
    private AsyncProcessingConfig createAsyncProcessingConfig(D2kListener annotation) {
        AsyncProcessingConfig config = new AsyncProcessingConfig();
        
        // 如果注解中启用了异步处理，使用注解中的配置
        if (annotation.asyncProcessing()) {
            config.setEnabled(true);
            config.setCorePoolSize(annotation.asyncCorePoolSize());
            config.setMaximumPoolSize(annotation.asyncMaxPoolSize());
            config.setQueueCapacity(annotation.asyncQueueCapacity());
            // 使用默认的保活时间和拒绝策略
            config.setKeepAliveTime(60L);
            config.setRejectedExecutionPolicy(AsyncProcessingConfig.RejectedExecutionPolicy.CALLER_RUNS);
        } else {
            // 如果注解中没有启用异步处理，检查全局配置
            D2kProperties.Consumer consumerProps = d2kProperties.getConsumer();
            config.setEnabled(consumerProps.isAsyncProcessingEnabled());
            config.setCorePoolSize(consumerProps.getAsyncCorePoolSize());
            config.setMaximumPoolSize(consumerProps.getAsyncMaximumPoolSize());
            config.setKeepAliveTime(consumerProps.getAsyncKeepAliveTime());
            config.setQueueCapacity(consumerProps.getAsyncQueueCapacity());
            
            // 解析拒绝策略字符串
            try {
                AsyncProcessingConfig.RejectedExecutionPolicy policy = 
                    AsyncProcessingConfig.RejectedExecutionPolicy.valueOf(
                        consumerProps.getAsyncRejectedExecutionPolicy().toUpperCase());
                config.setRejectedExecutionPolicy(policy);
            } catch (IllegalArgumentException e) {
                // 如果配置的策略无效，使用默认策略
                config.setRejectedExecutionPolicy(AsyncProcessingConfig.RejectedExecutionPolicy.CALLER_RUNS);
            }
        }
        
        return config;
    }

    private String generateContainerName(Class<?> beanClass, Method method, D2kListener annotation) {
        return beanClass.getSimpleName() + "." + method.getName() + "." + annotation.topic();
    }
}