package com.bizseer.hubble.mq.kafka.producer;

import com.bizseer.hubble.common.callback.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * 生产者
 *
 * @author: xl
 * @date: 2021/7/8
 **/
public class KafkaProducerSender {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerSender.class);


    private final Map<String, Object> producerProps;
    private KafkaProducer producer;


    public KafkaProducerSender(Map<String, Object> producerProps) {
        this.producerProps = producerProps;
    }

    public void init() {
        producer = new KafkaProducer(producerProps);
    }

    /**
     * 消息发送
     *
     * @param topic    队列
     * @param key      key
     * @param content  消息体
     * @param callback 回调
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    public Future send(String topic, String key, String content, Callback callback) {

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, content);
        return producer.send(record, ((metadata, exception) -> {
            if (exception == null) {
                logger.info("producer send success,key:{}", key);

                Optional.ofNullable(callback).ifPresent(Callback::success);
            } else {
                logger.error("producer send failure,key:{}, message: {}", key, exception);
                Optional.ofNullable(callback).ifPresent(cb -> cb.failure(exception));
            }
        }));
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
        if (producer != null) {
            producer.close();
        }
    }


}
