package com.bizseer.hubble.mq.kafka;

import com.bizseer.hubble.mq.MQClient;
import com.bizseer.hubble.mq.MQMessageReceiver;
import com.bizseer.hubble.mq.MQTopic;
import com.bizseer.hubble.mq.enums.MqType;
import com.bizseer.hubble.mq.kafka.manager.KafkaManager;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Future;

@Slf4j
public class KafkaMQClient implements MQClient {

    private final KafkaManager kafkaManager;

    public KafkaMQClient(KafkaManager kafkaManager) {
        this.kafkaManager = kafkaManager;
    }

    @Override
    public void addTopicIfNeeded(MQTopic topic) {
        KafkaManager.getInstance().addTopicIfNeeded(topic);
    }

    @Override
    public void subscribe(String group, String topic, MQMessageReceiver receiver) {
        KafkaManager.getInstance().subscribe(group, topic, receiver);
    }

    @Override
    public void publish(String topic, String message) {
        publish(topic, null, message);
    }

    @Override
    public Future publish(String topic, String key, String message) {
        return KafkaManager.getInstance().publish(topic, key, message);
    }


    @Override
    public void destory() {
        KafkaManager.getInstance().destroy();
    }


    @Override
    public MqType type() {
        return MqType.KAFKA;
    }
}
