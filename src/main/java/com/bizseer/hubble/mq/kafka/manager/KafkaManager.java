package com.bizseer.hubble.mq.kafka.manager;

import com.bizseer.hubble.mq.MQMessageReceiver;
import com.bizseer.hubble.mq.MQTopic;
import com.bizseer.hubble.mq.kafka.KafkaAdmin;
import com.bizseer.hubble.mq.kafka.consumer.KafkaConsumerWorker;
import com.bizseer.hubble.mq.kafka.producer.KafkaProducerSender;
import com.bizseer.hubble.mq.kafka.properties.KafkaProperties;
import com.bizseer.hubble.mq.thread.ThreadPoolExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;


/**
 * kafka管理
 *
 * @author: xl
 * @date: 2021/7/8
 **/
@Slf4j
public class KafkaManager {
    private static class InstanceHolder {
        public static final KafkaManager instance = new KafkaManager();
    }

    public static KafkaManager getInstance() {
        return InstanceHolder.instance;
    }

    private Executor executor = ThreadPoolExecutorUtil.getCommonIOPool();
    private KafkaProperties kafkaProperties = new KafkaProperties();
    private KafkaProducerSender sender;
    private KafkaAdmin admin;
    private ConcurrentMap<String, KafkaConsumerWorker> consumers;

    public KafkaManager() {
        this.consumers = new ConcurrentHashMap<>();
        init();
    }

    public void init() {
        Map<String, Object> producerProps = kafkaProperties.getProducerProperties();
        Map<String, Object> adminClientProps = kafkaProperties.getAdminClientProperties();
        sender = new KafkaProducerSender(producerProps);
        sender.init();
        admin = new KafkaAdmin(adminClientProps);
        log.info("KafkaManager#init config success!!!");
    }

    public void destroy() {
        if (sender != null) {
            sender.destroy();
        }
        if (executor instanceof ExecutorService) {
            ((ExecutorService) executor).shutdown();
        }
        consumers.forEach((k, consumer) -> consumer.destroy());
    }

    public void subscribe(String groupID, String topic, MQMessageReceiver receiver) {
        List<String> topics = Arrays.asList(topic.split(","));
        List<String> newTopics = topics.stream()
            .map(MQTopic::getTopic)
            .collect(Collectors.toList());
        Map<String, Object> properties = kafkaProperties.getConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        KafkaConsumerWorker consumer = new KafkaConsumerWorker(properties, newTopics, receiver);
        newTopics.forEach(t -> consumers.computeIfAbsent(t, k -> consumer));
        executor.execute(consumer);
    }

    public Future publish(String topic, String key, String message) {
        return sender.send(MQTopic.getTopic(topic), key, message, null);
    }

    public void addTopicIfNeeded(MQTopic topic) {
        admin.addTopicIfNeeded(topic);
    }


}
