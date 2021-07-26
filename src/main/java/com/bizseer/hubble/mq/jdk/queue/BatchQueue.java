package com.bizseer.hubble.mq.jdk.queue;

import lombok.Setter;

import java.util.concurrent.ConcurrentHashMap;

public class BatchQueue {

    @Setter
    public static volatile int batchSize = 5;

    /**
     * 默认手动提交
     */
    @Setter
    public static volatile boolean commitSync = true;


    public static int timeoutInMs = 500;//0.5秒检查一次

    private static ConcurrentHashMap<String, MessageQueue> msgQueueMap = new ConcurrentHashMap<>();

    /**
     * 添加队列信息
     *
     * @param
     * @return
     */
    public static boolean add(String topic, Object t) {
        MessageQueue messageQueue = getQueue(topic);
        boolean result = messageQueue.push(t);
        if (!messageQueue.get().get() && result) {
            messageQueue.set(true);
            messageQueue.signal();
        }
        return result;
    }


    /**
     * 根据group，topic获取队列
     *
     * @param topic
     * @return: com.bizseer.hubble.mq.jdk.queue.MessageQueue
     * @author: xl
     * @date: 2021/7/26
     **/
    public static MessageQueue getQueue(String topic) {
        return msgQueueMap.computeIfAbsent(topic, key -> {
            return new DefaultMQ();
        });
    }

}
