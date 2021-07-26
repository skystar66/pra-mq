package com.bizseer.hubble.mq.jdk.manager;

import com.bizseer.hubble.mq.MQMessageReceiver;
import com.bizseer.hubble.mq.jdk.consumer.ConsumerQueue;
import com.bizseer.hubble.mq.jdk.producer.MQProvider;
import com.bizseer.hubble.mq.thread.ThreadPoolExecutorUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;


/**
 * QUEUE 管理
 *
 * @author: xl
 * @date: 2021/7/8
 **/
@Slf4j
public class QueueManager {
    private static class InstanceHolder {
        public static final QueueManager instance = new QueueManager();
    }

    public static QueueManager getInstance() {
        return InstanceHolder.instance;
    }

    private Executor executor = ThreadPoolExecutorUtil.getCommonIOPool();
    private ConcurrentMap<String, ConsumerQueue> consumers;

    public QueueManager() {
        this.consumers = new ConcurrentHashMap<>();
    }

    public void destroy() {
        if (executor instanceof ExecutorService) {
            ((ExecutorService) executor).shutdown();
        }
        consumers.forEach((k, consumer) -> consumer.destroy());
    }

    public void subscribe(String topic, MQMessageReceiver receiver) {
        ConsumerQueue consumerQueue = new ConsumerQueue(receiver, topic);
        consumers.computeIfAbsent(topic, k -> consumerQueue);
        executor.execute(consumerQueue);
    }

    public boolean publish(String topic, Object message) {
        return MQProvider.pushMsg(topic, message);
    }


}
