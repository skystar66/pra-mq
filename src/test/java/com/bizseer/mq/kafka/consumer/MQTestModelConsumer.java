//package com.bizseer.mq.kafka.consumer;
//
//import com.bizseer.hubble.mq.MQMessageReceiver;
//import com.bizseer.mq.kafka.model.MQModel;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.TopicPartition;
//import org.springframework.stereotype.Component;
//
///**
// * 告警数据处理 带泛型的
// *
// * @author: xl
// * @date: 2021/7/8
// **/
//@Component("mq-model")
//@Slf4j
//public class MQTestModelConsumer implements MQMessageReceiver<MQModel> {
//
//
//    @Override
//    public void receive(String topic, MQModel message, Object... attachment) {
//        TopicPartition topicPartition = (TopicPartition) attachment[0];
//        OffsetAndMetadata offsetAndMetadata = (OffsetAndMetadata) attachment[1];
//
//        log.info("model receive topic:{}->msg:{}->partion:{}->offset:{}", topic, message.toString(),
//            topicPartition.partition(), offsetAndMetadata.offset());
//    }
//}
