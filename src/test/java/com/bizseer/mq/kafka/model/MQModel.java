package com.bizseer.mq.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * demo
 *
 * @author: xl
 * @date: 2021/7/8
 **/
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MQModel implements Serializable {


    private String id;
    private String name;
    private String age;
    private String six;

    @Override
    public String toString() {
        return "MQModel{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", age='" + age + '\'' +
            ", six='" + six + '\'' +
            '}';
    }
}
