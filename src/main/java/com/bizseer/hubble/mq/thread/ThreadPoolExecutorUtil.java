package com.bizseer.hubble.mq.thread;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * 线程池工具类
 *
 * @author: xl
 * @date: 2021/7/5
 **/
@Slf4j
public class ThreadPoolExecutorUtil {


    @Getter
    private final static ExecutorService commonIOPool = new ThreadPoolExecutor(50, 200,
        1L,
        TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(100),
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "kafka-consumer-pool");
                thread.setDaemon(true);
                return thread;
            }
        },
        new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.warn("kafka-consumer-pool 丢弃。");
            }
        }
    );


    @Getter
    private final static ExecutorService commonCPUPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
        Runtime.getRuntime().availableProcessors() * 2,
        1L,
        TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(100),
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "jdk-consumer-pool");
                thread.setDaemon(true);
                return thread;
            }
        },
        new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.warn("jdk-consumer-pool 丢弃。");
            }
        }
    );
}
