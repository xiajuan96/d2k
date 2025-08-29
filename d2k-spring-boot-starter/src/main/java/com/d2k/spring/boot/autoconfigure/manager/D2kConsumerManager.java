package com.d2k.spring.boot.autoconfigure.manager;

import com.d2k.consumer.DelayConsumerContainer;
import org.springframework.beans.factory.DisposableBean;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * D2K 消费者管理器
 * 用于管理多个 DelayConsumerContainer 实例的生命周期
 */
public class D2kConsumerManager implements DisposableBean {

    private final ConcurrentMap<String, DelayConsumerContainer<?, ?>> containers = new ConcurrentHashMap<>();

    /**
     * 注册消费者容器
     *
     * @param name 容器名称
     * @param container 消费者容器
     */
    public void registerContainer(String name, DelayConsumerContainer<?, ?> container) {
        containers.put(name, container);
    }

    /**
     * 获取消费者容器
     *
     * @param name 容器名称
     * @return 消费者容器
     */
    public DelayConsumerContainer<?, ?> getContainer(String name) {
        return containers.get(name);
    }

    /**
     * 启动指定的消费者容器
     *
     * @param name 容器名称
     */
    public void startContainer(String name) {
        DelayConsumerContainer<?, ?> container = containers.get(name);
        if (container != null) {
            container.start();
        }
    }

    /**
     * 停止指定的消费者容器
     *
     * @param name 容器名称
     */
    public void stopContainer(String name) {
        DelayConsumerContainer<?, ?> container = containers.get(name);
        if (container != null) {
            container.stop();
        }
    }

    /**
     * 启动所有消费者容器
     */
    public void startAllContainers() {
        containers.values().forEach(DelayConsumerContainer::start);
    }

    /**
     * 停止所有消费者容器
     */
    public void stopAllContainers() {
        containers.values().forEach(DelayConsumerContainer::stop);
    }

    /**
     * 获取所有容器名称
     *
     * @return 容器名称集合
     */
    public java.util.Set<String> getContainerNames() {
        return containers.keySet();
    }

    /**
     * 应用关闭时停止所有容器
     */
    @Override
    public void destroy() throws Exception {
        stopAllContainers();
    }
}