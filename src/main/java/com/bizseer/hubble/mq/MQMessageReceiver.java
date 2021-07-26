package com.bizseer.hubble.mq;

/**
 * 接受者对消息进行逻辑处理，对某个队列进行消费的只需要实现该接口即可
 *
 * @author: xl
 * @date: 2021/7/8
 **/
public interface MQMessageReceiver<T> {


    /**
     * receive
     *
     * @param topic      队列
     * @param message    消息
     * @param attachment TopicPartition and OffsetAndMetadata 返回消费队列的partition and  offset索引
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    void receive(String topic, T message, Object... attachment);

}
