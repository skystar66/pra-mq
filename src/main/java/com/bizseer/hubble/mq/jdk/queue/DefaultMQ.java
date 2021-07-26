package com.bizseer.hubble.mq.jdk.queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultMQ<T> implements MessageQueue<T> {
    private final LinkedBlockingQueue<T> msgQueue;

    private AtomicBoolean isLoopingStock = new AtomicBoolean(false);

    private Lock lock;

    private Condition condition;


    public DefaultMQ() {
        msgQueue = new LinkedBlockingQueue<>(100000);
        lock = new ReentrantLock(true);
        condition = lock.newCondition();
    }

    @Override
    public boolean push(T msg) {
        return msgQueue.offer(msg);
    }

    @Override
    public T pop() {
        T msg = null;

        try {
            msg = msgQueue.take();
        } catch (InterruptedException ignore) {
        }

        return msg;
    }

    @Override
    public int size() {
        return msgQueue.size();
    }


    @Override
    public AtomicBoolean get() {
        return isLoopingStock;
    }

    @Override
    public void set(boolean isLoop) {
        isLoopingStock.set(isLoop);
    }


    @Override
    public boolean isEmpty() {
        return msgQueue.isEmpty();
    }


    @Override
    public void drainTo(List<Object> msgs, int size) {
        msgQueue.drainTo(msgs, size);
    }

    @Override
    public void await() {
        try {
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void signal() {
        try {
            lock.lock();
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

}
