package com.d2k.utils;

import java.util.Map;

/**
 * 配置工具类，提供通用的配置值获取和类型转换方法
 * 
 * @author xiajuan96
 */
public class ConfigUtils {
    
    /**
     * 从配置Map中获取long类型配置值
     * 
     * @param configs 配置Map
     * @param key 配置键
     * @param defaultValue 默认值
     * @return long类型的配置值
     * @throws IllegalArgumentException 当配置值类型不正确或格式无效时
     */
    public static long getLongConfig(Map<String, Object> configs, String key, long defaultValue) {
        return getNumberConfig(configs, key, defaultValue, Long.class, 
            value -> {
                if (value instanceof String) {
                    try {
                        return Long.parseLong((String) value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid long value for key '" + key + "': " + value, e);
                    }
                }
                return ((Number) value).longValue();
            });
    }
    
    /**
     * 从配置Map中获取int类型配置值
     * 
     * @param configs 配置Map
     * @param key 配置键
     * @param defaultValue 默认值
     * @return int类型的配置值
     * @throws IllegalArgumentException 当配置值类型不正确或格式无效时
     */
    public static int getIntConfig(Map<String, Object> configs, String key, int defaultValue) {
        return getNumberConfig(configs, key, defaultValue, Integer.class,
            value -> {
                if (value instanceof String) {
                    try {
                        return Integer.parseInt((String) value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid int value for key '" + key + "': " + value, e);
                    }
                }
                return ((Number) value).intValue();
            });
    }
    
    /**
     * 从配置Map中获取double类型配置值
     * 
     * @param configs 配置Map
     * @param key 配置键
     * @param defaultValue 默认值
     * @return double类型的配置值
     * @throws IllegalArgumentException 当配置值类型不正确或格式无效时
     */
    public static double getDoubleConfig(Map<String, Object> configs, String key, double defaultValue) {
        return getNumberConfig(configs, key, defaultValue, Double.class,
            value -> {
                if (value instanceof String) {
                    try {
                        return Double.parseDouble((String) value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid double value for key '" + key + "': " + value, e);
                    }
                }
                return ((Number) value).doubleValue();
            });
    }
    
    /**
     * 通用的数值类型配置获取方法
     * 
     * @param configs 配置Map
     * @param key 配置键
     * @param defaultValue 默认值
     * @param targetType 目标类型
     * @param converter 类型转换器
     * @param <T> 目标类型泛型
     * @return 转换后的配置值
     * @throws IllegalArgumentException 当配置值类型不正确时
     */
    private static <T extends Number> T getNumberConfig(Map<String, Object> configs, String key, 
                                                        T defaultValue, Class<T> targetType, 
                                                        NumberConverter<T> converter) {
        Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        
        if (value instanceof Number || value instanceof String) {
            return converter.convert(value);
        }
        
        throw new IllegalArgumentException("Invalid type for key '" + key + "', expected Number or String, got: " 
            + value.getClass().getSimpleName());
    }
    
    /**
     * 数值转换器函数式接口
     * 
     * @param <T> 目标数值类型
     */
    @FunctionalInterface
    private interface NumberConverter<T> {
        T convert(Object value);
    }
}