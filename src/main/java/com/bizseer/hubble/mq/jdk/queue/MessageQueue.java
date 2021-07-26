package com.bizseer.hubble.mq.jdk.queue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MessageQueue<T> {

    boolean push(T msg);

    T pop();

    int size();

    AtomicBoolean get();

    void set(boolean isLoop);


    boolean isEmpty();


    void drainTo(List<Object> msgs, int size);
    void await();

    void signal();

}
