package com.bizseer.hubble.mq.jdk.consumer;

import com.bizseer.hubble.mq.MQMessageReceiver;
import com.bizseer.hubble.mq.jdk.queue.BatchQueue;
import com.bizseer.hubble.mq.jdk.queue.MessageQueue;
import com.bizseer.hubble.mq.thread.ThreadPoolExecutorUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ConsumerQueue implements Runnable {

    private volatile boolean running = true;
    private final MQMessageReceiver receiver;
    private final String topic;

    private MessageQueue queue;
    private AtomicLong start = new AtomicLong(System.currentTimeMillis());

    public ConsumerQueue(MQMessageReceiver receiver, String topic) {
        this.receiver = receiver;
        this.topic = topic;
        this.queue = BatchQueue.getQueue(topic);
    }

    @Override
    public void run() {
        start = new AtomicLong(System.currentTimeMillis());
        while (isRunning()) {
            try {
                long last = System.currentTimeMillis() - start.get();
                if (queue.size() >= BatchQueue.batchSize || (!queue.isEmpty() && last > BatchQueue.timeoutInMs)) {
                    /**开始业务逻辑处理*/
                    drainToConsumer();
                } else if (queue.isEmpty()) {
                    //如果队列中的元素为空，则isLooping 计数从0开始
                    queue.set(false);
                    queue.await();
                }
            } catch (Exception e) {
            }
        }
    }

    /**
     * 逻辑处理
     *
     * @param
     * @return
     */

    private void drainToConsumer() {
        List<Object> msgs = new ArrayList<>();
        queue.drainTo(msgs, BatchQueue.batchSize);
        if (BatchQueue.commitSync) {
            /**手动提交*/
            List<CompletableFuture> completableFutures = new ArrayList<>(msgs.size());
            for (Object msg : msgs) {
                completableFutures.add(CompletableFuture.runAsync(() -> {
                    receiver.receive(topic, msg);
                }, ThreadPoolExecutorUtil.getCommonCPUPool()));
            }
            CompletableFuture<Void> allFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
            allFuture.join();
        } else {
            /**自动提交{提升消息并行度}*/
            for (Object msg : msgs) {
                ThreadPoolExecutorUtil.getCommonCPUPool().execute(() -> {
                    receiver.receive(topic, msg);
                });
            }
        }
        start.set(System.currentTimeMillis());
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
