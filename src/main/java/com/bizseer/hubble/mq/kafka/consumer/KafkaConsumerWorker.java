package com.bizseer.hubble.mq.kafka.consumer;


import com.bizseer.hubble.common.json.JsonUtil;
import com.bizseer.hubble.common.util.StringUtils;
import com.bizseer.hubble.mq.MQMessageReceiver;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;

/**
 * 消费者
 *
 * @author: xl
 * @date: 2021/7/8
 **/
public class KafkaConsumerWorker implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWorker.class);
    private List<String> topics;
    private volatile boolean running = true;
    private final MQMessageReceiver receiver;
    private final KafkaConsumer consumer;


    public KafkaConsumerWorker(Map<String, Object> properties, List<String> topics, MQMessageReceiver receiver) {
        this.topics = topics;
        this.receiver = receiver;
        this.consumer = new KafkaConsumer(properties);
        LOGGER.info("Listener MQ Topic :{}", topics);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (isRunning()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(20));
                    records.forEach(record -> {
                        LOGGER.info("topic: {} partition: {} offset: {} ", record.topic(), record.partition(), record.offset());
                        if (StringUtils.isNotBlank(record.value())) {
                            Type[] types = receiver.getClass().getGenericInterfaces();
                            if (types[0] instanceof ParameterizedType) {
                                ParameterizedType parameterized = (ParameterizedType) types[0];
                                if (parameterized != null) {
                                    Class<?> clazz = (Class<?>) parameterized.getActualTypeArguments()[0];
                                    if (clazz != null) {
                                        receiver.receive(record.topic(), JsonUtil.fromJson(record.value(), clazz),
                                            new TopicPartition(record.topic(), record.partition()),
                                            new OffsetAndMetadata(record.offset() + 1));
                                        return;
                                    }
                                }
                            }
                            /**没有泛型 默认string*/
                            receiver.receive(record.topic(), record.value(),
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                        }
                    });
                    consumer.commitAsync();
                } catch (Exception ex) {
                    LOGGER.error("json serialize  failure, err: {}", ex.getMessage());
                }
            }

        } catch (Exception e) {
            LOGGER.error("consume failure, err: {}", e.getMessage());
        } finally {
            consumer.close();
        }
    }


    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * 销毁
     *
     * @param
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    public void destroy() {
        setRunning(false);
    }
}
