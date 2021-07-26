package com.bizseer.hubble.mq;

import com.bizseer.hubble.mq.enums.MqType;

import java.util.concurrent.Future;

/**
 * MQ通用客户端
 *
 * @author: xl
 * @date: 2021/7/8
 **/
public interface MQClient {

    /**
     * 添加topic
     *
     * @param topic
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    void addTopicIfNeeded(MQTopic topic);

    /**
     * 注册topic 消费者
     *
     * @param group    消费组
     * @param topic    队列
     * @param receiver
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    void subscribe(String group, String topic, MQMessageReceiver receiver);


    /**
     * 注册topic 消费者
     *
     * @param topic    队列
     * @param receiver
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    default void subscribe(String topic, MQMessageReceiver receiver) {
    }

    /**
     * 推送消息
     *
     * @param topic
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    void publish(String topic, String message);

    /**
     * 推送消息
     *
     * @param topic
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    Future publish(String topic, String key, String message);


    /**
     * 推送消息
     *
     * @param message
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    default boolean publish(String topic, Object message) {
        return true;
    }


    /**
     * 销毁
     *
     * @param
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    void destory();


    /**
     * MQ 类型
     *
     * @param
     * @return: com.bizseer.hubble.mq.enums.MqType
     * @author: xl
     * @date: 2021/7/8
     **/
    MqType type();


}
