package com.bizseer.hubble.mq.jdk.producer;

import com.bizseer.hubble.mq.jdk.queue.BatchQueue;

public class MQProvider {


    /**
     * 推送消息
     *
     * @param msg
     * @return: void
     * @author: xl
     * @date: 2021/7/23
     **/
    public static boolean pushMsg(String topic, Object msg) {
        return BatchQueue.add(topic, msg);
    }


    /**
     * 更新mq配置
     *
     * @param concurrentThreshold
     * @return: void
     * @author: xl
     * @date: 2021/7/23
     **/
    public static void updateMQConfig(int concurrentThreshold) {
        BatchQueue.setBatchSize(concurrentThreshold);
    }

}
