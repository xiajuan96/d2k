package com.d2k.spring.boot.autoconfigure.annotation;

import com.d2k.spring.boot.autoconfigure.config.D2kAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 启用 D2K 延迟消息功能的注解
 * 在 Spring Boot 应用的主类上使用此注解来启用 D2K 功能
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(D2kAutoConfiguration.class)
public @interface EnableD2k {
}