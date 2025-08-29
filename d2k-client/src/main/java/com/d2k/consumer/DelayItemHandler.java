package com.d2k.consumer;

/**
 * 延迟消息处理接口
 * 
 * 当队列中的消息准备好被处理时，调用此接口的实现类来处理消息。
 * 实现类需要处理消息的业务逻辑，例如数据库操作、消息发送等。
 * 
 * @author xiajuan96
 * @date 2025/8/13 16:08    
 */
public interface DelayItemHandler<K,V> {
    /**
     * 处理延迟消息
     * 
     * 当队列中的消息准备好被处理时调用此方法。
     * 
     * @param delayItem 要处理的延迟消息项
     */
    void process(DelayItem<K,V> delayItem);
}
