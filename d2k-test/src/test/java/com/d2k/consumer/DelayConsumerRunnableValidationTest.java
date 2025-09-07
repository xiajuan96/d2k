package com.d2k.consumer;

import com.d2k.consumer.DelayConsumerRunnable;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.*;

/**
 * DelayConsumerRunnable参数校验测试
 * 
 * @author xiajuan96
 */
public class DelayConsumerRunnableValidationTest {

    private Map<String, Object> validConfigs;
    private Collection<String> topics;

    @Before
    public void setUp() {
        validConfigs = new HashMap<>();
        validConfigs.put("bootstrap.servers", "localhost:9092");
        validConfigs.put("group.id", "test-group");
        validConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        validConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        topics = Arrays.asList("test-topic");
    }

    @Test
    public void testValidConfigs() {
        // 正常情况应该不抛异常
        try {
            new DelayConsumerRunnable<String, String>(validConfigs, topics, item -> {});
        } catch (Exception e) {
            fail("不应该抛出异常: " + e.getMessage());
        }
    }

    @Test
    public void testNullConfigs() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>((Map<String, Object>) null, topics, item -> {});
        });
        assertEquals("配置参数不能为null", exception.getMessage());
    }

    @Test
    public void testMissingBootstrapServers() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.remove("bootstrap.servers");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {});
        });
        assertEquals("缺少必传参数: bootstrap.servers", exception.getMessage());
    }

    @Test
    public void testMissingGroupId() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.remove("group.id");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, (AsyncProcessingConfig) null);
        });
        assertEquals("缺少必传参数: group.id", exception.getMessage());
    }

    @Test
    public void testMissingKeyDeserializer() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.remove("key.deserializer");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertEquals("缺少必传参数: key.deserializer", exception.getMessage());
    }

    @Test
    public void testMissingValueDeserializer() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.remove("value.deserializer");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertEquals("缺少必传参数: value.deserializer", exception.getMessage());
    }

     @Test
     public void testEmptyBootstrapServers() {
         Map<String, Object> configs = new HashMap<>(validConfigs);
         configs.put("bootstrap.servers", "");
         
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
             new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, (AsyncProcessingConfig) null);
         });
         assertEquals("参数值不能为空: bootstrap.servers", exception.getMessage());
    }

    @Test
    public void testInvalidBootstrapServersFormat() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("bootstrap.servers", "localhost"); // 缺少端口
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertTrue(exception.getMessage().contains("格式错误，应为host:port格式"));
    }

    @Test
    public void testInvalidBootstrapServersPort() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("bootstrap.servers", "localhost:99999"); // 端口超出范围
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertTrue(exception.getMessage().contains("端口号无效，应在1-65535范围内"));
    }

    @Test
    public void testInvalidBootstrapServersPortFormat() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("bootstrap.servers", "localhost:abc"); // 端口不是数字
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertTrue(exception.getMessage().contains("端口号格式错误，应为数字"));
    }

    @Test
    public void testInvalidDeserializerClass() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("key.deserializer", "com.nonexistent.Deserializer");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertTrue(exception.getMessage().contains("指定的类不存在"));
    }

    @Test
    public void testMultipleBootstrapServers() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        
        // 多个服务器地址应该正常工作
        try {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        } catch (Exception e) {
            fail("不应该抛出异常: " + e.getMessage());
        }
    }

    @Test
    public void testMultipleBootstrapServersWithInvalidOne() {
        Map<String, Object> configs = new HashMap<>(validConfigs);
        configs.put("bootstrap.servers", "localhost:9092,invalid-server,localhost:9094");
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new DelayConsumerRunnable<String, String>(configs, topics, item -> {}, null);
        });
        assertTrue(exception.getMessage().contains("格式错误，应为host:port格式"));
    }
}