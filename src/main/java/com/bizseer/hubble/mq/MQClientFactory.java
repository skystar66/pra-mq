package com.bizseer.hubble.mq;

import com.bizseer.hubble.mq.enums.MqType;
import com.bizseer.hubble.mq.jdk.QueueClient;
import com.bizseer.hubble.mq.kafka.KafkaMQClient;
import com.bizseer.hubble.mq.kafka.manager.KafkaManager;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

public class MQClientFactory {
    private static Map<MqType, MQClient> mqClientMap = new HashMap<>();

    private static class InstanceHolder {
        public static final MQClientFactory instance = new MQClientFactory();
    }

    public static MQClientFactory getInstance() {
        return InstanceHolder.instance;
    }

    public MQClientFactory() {
        init();
    }

    /**
     * 初始化操作
     */
    public void init() {
        mqClientMap.put(MqType.KAFKA, new KafkaMQClient(KafkaManager.getInstance()));
        mqClientMap.put(MqType.JDK, new QueueClient());
    }

    public MQClient getMQClient(MqType mqType) {
        return mqClientMap.get(mqType);
    }


}
