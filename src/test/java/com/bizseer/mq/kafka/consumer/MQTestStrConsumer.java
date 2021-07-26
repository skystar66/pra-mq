//package com.bizseer.mq.kafka.consumer;
//
//import com.bizseer.hubble.mq.MQMessageReceiver;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.TopicPartition;
//import org.springframework.stereotype.Component;
//
///**
// * 告警数据处理 默认string类型的
// *
// * @author: xl
// * @date: 2021/7/8
// **/
//@Component("mq-str")
//@Slf4j
//public class MQTestStrConsumer implements MQMessageReceiver {
//
//
//    @Override
//    public void receive(String topic, Object message, Object... attachment) {
//        TopicPartition topicPartition = (TopicPartition) attachment[0];
//        OffsetAndMetadata offsetAndMetadata = (OffsetAndMetadata) attachment[1];
//
//        log.info("str receive topic:{}->msg:{}->partion:{}->offset:{}", topic, (String) message,
//            topicPartition.partition(), offsetAndMetadata.offset());
//    }
//}
