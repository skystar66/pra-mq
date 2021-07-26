package com.bizseer.hubble.mq.jdk;

import com.bizseer.hubble.mq.MQClient;
import com.bizseer.hubble.mq.MQMessageReceiver;
import com.bizseer.hubble.mq.MQTopic;
import com.bizseer.hubble.mq.enums.MqType;
import com.bizseer.hubble.mq.jdk.manager.QueueManager;

import java.util.concurrent.Future;

public class QueueClient implements MQClient {
    @Override
    public void addTopicIfNeeded(MQTopic topic) {

    }

    @Override
    public void subscribe(String group, String topic, MQMessageReceiver receiver) {

    }

    @Override
    public void subscribe(String topic, MQMessageReceiver receiver) {
        QueueManager.getInstance().subscribe(topic, receiver);
    }

    @Override
    public void publish(String topic, String message) {

    }

    @Override
    public boolean publish(String topic, Object message) {
        return QueueManager.getInstance().publish(topic, message);
    }

    @Override
    public Future publish(String topic, String key, String message) {
        return null;
    }

    @Override
    public void destory() {
        QueueManager.getInstance().destroy();
    }

    @Override
    public MqType type() {
        return null;
    }
}
