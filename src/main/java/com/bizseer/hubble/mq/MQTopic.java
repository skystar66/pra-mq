package com.bizseer.hubble.mq;

import lombok.Data;

/**
 * 消息队列：mqTopic{topic,numPartitions}
 *
 * @author: xl
 * @date: 2021/7/8
 **/
@Data
public class MQTopic {

    private final String topic;
    private final int numPartitions;

    public MQTopic(String topic) {
        this(topic, 1);
    }

    public MQTopic(String topic, int numPartitions) {
        this.topic = topic;
        this.numPartitions = numPartitions;
    }

    public static String getTopic(String topic) {
        return topic;
    }
}
