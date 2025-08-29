package com.d2k.spring.boot.autoconfigure.example;

import com.d2k.spring.boot.autoconfigure.annotation.D2kListener;
import com.d2k.spring.boot.autoconfigure.annotation.EnableD2k;
import com.d2k.spring.boot.autoconfigure.template.D2kTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * D2K Spring集成示例应用
 * 演示如何在Spring Boot应用中使用D2K延迟消息功能
 * 应用将在启动1分钟后自动关闭
 */
@SpringBootApplication
@EnableD2k
@EnableScheduling
public class D2kSpringExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(D2kSpringExampleApplication.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static ConfigurableApplicationContext applicationContext;
    private static final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

    public static void main(String[] args) {
        log.info("[{}] D2K Spring示例应用启动中...", LocalDateTime.now().format(formatter));
        applicationContext = SpringApplication.run(D2kSpringExampleApplication.class, args);
        log.info("[{}] D2K Spring示例应用启动完成，将在60秒后自动关闭", LocalDateTime.now().format(formatter));
    }

    /**
     * 应用自动关闭组件
     * 在应用启动1分钟后执行关闭操作
     */
    @Component
    public static class ApplicationShutdownScheduler {

        private final Logger log = LoggerFactory.getLogger(ApplicationShutdownScheduler.class);
        private final ApplicationContext context;
        private long startTime;

        @Autowired
        public ApplicationShutdownScheduler(ApplicationContext context) {
            this.context = context;
            this.startTime = System.currentTimeMillis();
            log.info("[{}] 应用自动关闭调度器已初始化，启动时间: {}", 
                LocalDateTime.now().format(formatter), 
                LocalDateTime.now().format(formatter));
        }

        /**
         * 定时任务：每10秒检查一次，在启动60秒后关闭应用
         */
        @Scheduled(fixedRate = 10000) // 每10秒执行一次
        public void checkShutdown() {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime;
            long remainingTime = 60000 - elapsedTime; // 60秒 = 60000毫秒

            if (remainingTime <= 0 && shutdownInitiated.compareAndSet(false, true)) {
                log.info("[{}] 应用运行时间已达到60秒，开始执行关闭流程...", 
                    LocalDateTime.now().format(formatter));
                initiateShutdown();
            } else if (remainingTime > 0) {
                log.info("[{}] 应用运行中，剩余时间: {}秒", 
                    LocalDateTime.now().format(formatter), 
                    remainingTime / 1000);
            }
        }

        /**
         * 执行应用关闭流程
         */
        private void initiateShutdown() {
            try {
                log.info("[{}] ========== 开始应用关闭流程 ==========", 
                    LocalDateTime.now().format(formatter));
                
                // 记录关闭前的系统状态
                logSystemStatus();
                
                // 记录Spring容器中的Bean信息
                logBeanInformation();
                
                // 优雅关闭应用
                log.info("[{}] 正在关闭Spring应用上下文...", 
                    LocalDateTime.now().format(formatter));
                
                // 在新线程中执行关闭操作，避免阻塞定时任务线程
                new Thread(() -> {
                    try {
                        Thread.sleep(1000); // 给日志输出一些时间
                        log.info("[{}] 执行System.exit(0)关闭应用", 
                            LocalDateTime.now().format(formatter));
                        System.exit(0);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("关闭线程被中断", e);
                    }
                }, "shutdown-thread").start();
                
            } catch (Exception e) {
                log.error("[{}] 应用关闭过程中发生异常", 
                    LocalDateTime.now().format(formatter), e);
            }
        }

        /**
         * 记录系统状态信息
         */
        private void logSystemStatus() {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            
            log.info("[{}] 系统资源状态:", LocalDateTime.now().format(formatter));
            log.info("  - 总内存: {} MB", totalMemory / 1024 / 1024);
            log.info("  - 已用内存: {} MB", usedMemory / 1024 / 1024);
            log.info("  - 空闲内存: {} MB", freeMemory / 1024 / 1024);
            log.info("  - 可用处理器数: {}", runtime.availableProcessors());
            log.info("  - 活跃线程数: {}", Thread.activeCount());
        }

        /**
         * 记录Spring容器Bean信息
         */
        private void logBeanInformation() {
            if (context instanceof ConfigurableApplicationContext) {
                ConfigurableApplicationContext configurableContext = (ConfigurableApplicationContext) context;
                String[] beanNames = configurableContext.getBeanDefinitionNames();
                
                log.info("[{}] Spring容器信息:", LocalDateTime.now().format(formatter));
                log.info("  - Bean总数: {}", beanNames.length);
                log.info("  - 容器是否活跃: {}", configurableContext.isActive());
                log.info("  - 容器是否正在运行: {}", configurableContext.isRunning());
                
                // 记录D2K相关的Bean
                log.info("  - D2K相关Bean:");
                for (String beanName : beanNames) {
                    if (beanName.toLowerCase().contains("d2k")) {
                        try {
                            Object bean = configurableContext.getBean(beanName);
                            log.info("    * {}: {}", beanName, bean.getClass().getSimpleName());
                        } catch (Exception e) {
                            log.warn("    * {}: 无法获取Bean实例 - {}", beanName, e.getMessage());
                        }
                    }
                }
            }
        }
    }

    /**
     * 消息生产者示例
     */
    @Component
    public static class MessageProducer implements CommandLineRunner {

        private final Logger log = LoggerFactory.getLogger(MessageProducer.class);

        @Autowired
        private D2kTemplate<String, String> d2kTemplate;

        @Override
        public void run(String... args) throws Exception {
            log.info("[{}] 消息生产者组件启动，等待2秒后开始发送消息...", 
                LocalDateTime.now().format(formatter));
            
            // 等待消费者启动
            Thread.sleep(2000);

            log.info("[{}] 开始发送延迟消息，监控资源使用情况...", 
                LocalDateTime.now().format(formatter));
            
            // 记录发送前的系统状态
            logResourceStatus("消息发送前");

            // 注意：需要在配置文件中预配置 test-topic 的延迟时间
            try {
                // 发送延迟消息（使用预配置的延迟时间）
                long startTime = System.currentTimeMillis();
                d2kTemplate.send("test-topic", "key1", "这是一条延迟消息");
                long endTime = System.currentTimeMillis();
                log.info("[{}] 发送延迟消息完成: 使用预配置延迟时间，耗时: {}ms", 
                    LocalDateTime.now().format(formatter), endTime - startTime);

                // 异步发送消息
                startTime = System.currentTimeMillis();
                d2kTemplate.sendAsync("test-topic", "key2", "这是一条异步消息");
                endTime = System.currentTimeMillis();
                log.info("[{}] 发送异步消息完成，耗时: {}ms", 
                    LocalDateTime.now().format(formatter), endTime - startTime);

                // 同步发送消息
                startTime = System.currentTimeMillis();
                d2kTemplate.sendSync("test-topic", "key3", "这是一条同步消息");
                endTime = System.currentTimeMillis();
                log.info("[{}] 发送同步消息完成，耗时: {}ms", 
                    LocalDateTime.now().format(formatter), endTime - startTime);

                // 批量发送延迟消息
                log.info("[{}] 开始批量发送3条延迟消息...", 
                    LocalDateTime.now().format(formatter));
                for (int i = 0; i < 3; i++) {
                    startTime = System.currentTimeMillis();
                    d2kTemplate.send("test-topic", "batch-key-" + i,
                            "批量延迟消息 #" + (i + 1));
                    endTime = System.currentTimeMillis();
                    log.info("[{}] 发送批量延迟消息 #{} 完成，耗时: {}ms", 
                        LocalDateTime.now().format(formatter), (i + 1), endTime - startTime);
                }
                
                // 记录发送后的系统状态
                logResourceStatus("消息发送后");
                
            } catch (IllegalArgumentException e) {
                log.error("[{}] 发送消息失败: {}", 
                    LocalDateTime.now().format(formatter), e.getMessage());
                log.error("请在配置文件中为 test-topic 配置延迟时间");
            } catch (Exception e) {
                log.error("[{}] 消息发送过程中发生异常", 
                    LocalDateTime.now().format(formatter), e);
            }
        }
        
        /**
         * 记录资源使用状态
         */
        private void logResourceStatus(String phase) {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            
            log.info("[{}] {} - 资源状态: 已用内存={}MB, 空闲内存={}MB, 活跃线程={}", 
                LocalDateTime.now().format(formatter),
                phase,
                usedMemory / 1024 / 1024,
                freeMemory / 1024 / 1024,
                Thread.activeCount());
        }
    }

    /**
     * 消息消费者示例
     */
    @Component
    public static class MessageConsumer {

        private final Logger log = LoggerFactory.getLogger(MessageConsumer.class);
        private volatile int messageCount = 0;

        /**
         * 处理延迟消息 - 使用ConsumerRecord参数（完整消息信息）
         */
        @D2kListener(topic = "test-topic", groupId = "test-group-1")
        public void handleDelayMessage(ConsumerRecord<String, String> record) {
            long startTime = System.currentTimeMillis();
            messageCount++;
            
            try {
                log.info("[{}] 收到延迟消息 #{}: "
                                + "\n  Topic: {}"
                                + "\n  Key: {}"
                                + "\n  Value: {}"
                                + "\n  Partition: {}"
                                + "\n  Offset: {}"
                                + "\n  Headers: {}"
                                + "\n  消息时间戳: {}"
                                + "\n  消费延迟: {}ms"
                                + "\n  累计处理消息数: {}"
                        ,
                        LocalDateTime.now().format(formatter), messageCount,
                        record.topic(), record.key(), record.value(), 
                        record.partition(), record.offset(), record.headers(),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault()).format(formatter),
                        System.currentTimeMillis() - record.timestamp(),
                        messageCount
                );
                
                // 模拟消息处理时间
                Thread.sleep(100);
                
                long endTime = System.currentTimeMillis();
                log.info("[{}] 延迟消息 #{} 处理完成，处理耗时: {}ms", 
                    LocalDateTime.now().format(formatter), messageCount, endTime - startTime);
                    
                // 每处理5条消息记录一次资源状态
                if (messageCount % 5 == 0) {
                    logResourceStatus("处理第" + messageCount + "条消息后");
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("[{}] 消息处理被中断", LocalDateTime.now().format(formatter), e);
            } catch (Exception e) {
                log.error("[{}] 处理延迟消息时发生异常", LocalDateTime.now().format(formatter), e);
            }
        }

        /**
         * 处理延迟消息 - 使用String参数（仅消息内容）
         */
        @D2kListener(topic = "another-topic", groupId = "test-group-2", concurrency = 2)
        public void handleSimpleMessage(String message) {
            long startTime = System.currentTimeMillis();
            try {
                log.info("[{}] 收到简单消息: {}", LocalDateTime.now().format(formatter), message);
                Thread.sleep(50); // 模拟处理时间
                long endTime = System.currentTimeMillis();
                log.info("[{}] 简单消息处理完成，耗时: {}ms", 
                    LocalDateTime.now().format(formatter), endTime - startTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("[{}] 简单消息处理被中断", LocalDateTime.now().format(formatter), e);
            }
        }

        /**
         * 处理延迟消息 - 无参数方法
         */
        @D2kListener(topic = "notification-topic", groupId = "notification-group")
        public void handleNotification() {
            log.info("[{}] 收到通知消息，当前线程: {}", 
                LocalDateTime.now().format(formatter), Thread.currentThread().getName());
        }
//
//        /**
//         * 处理延迟消息 - 使用Object参数（泛型支持）
//         */
//        @D2kListener(topic = "object-topic", groupId = "object-group")
//        public void handleObjectMessage(Object message) {
//            log.info("[{}] 收到Object类型消息: {} (类型: {})",
//                LocalDateTime.now().format(formatter),
//                message,
//                (message != null ? message.getClass().getSimpleName() : "null"));
//        }

//        /**
//         * 处理延迟消息 - 使用Integer参数（数值类型转换）
//         */
//        @D2kListener(topic = "number-topic", groupId = "number-group")
//        public void handleNumberMessage(Integer number) {
//            log.info("[{}] 收到数值消息: {}", LocalDateTime.now().format(formatter), number);
//        }
        
        /**
         * 记录资源使用状态
         */
        private void logResourceStatus(String phase) {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            
            log.info("[{}] {} - 消费者资源状态: 已用内存={}MB, 空闲内存={}MB, 活跃线程={}", 
                LocalDateTime.now().format(formatter),
                phase,
                usedMemory / 1024 / 1024,
                freeMemory / 1024 / 1024,
                Thread.activeCount());
        }
    }
}