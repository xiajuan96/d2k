package com.d2k.spring.boot.autoconfigure.config;

import com.d2k.spring.boot.autoconfigure.D2kProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * D2K 自动配置类
 * 导入 Producer 和 Consumer 的独立配置类
 */
@Configuration
@EnableConfigurationProperties(D2kProperties.class)
@Import({D2kProducerAutoConfiguration.class, D2kConsumerAutoConfiguration.class})
public class D2kAutoConfiguration {


}