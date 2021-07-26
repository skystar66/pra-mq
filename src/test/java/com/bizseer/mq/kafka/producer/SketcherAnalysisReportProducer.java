//package com.bizseer.mq.kafka.producer;
//
//import com.bizseer.hubble.common.json.JsonUtil;
//import com.bizseer.hubble.mq.MQClient;
//import com.bizseer.hubble.mq.MQClientFactory;
//import com.bizseer.hubble.mq.enums.MqType;
//import com.bizseer.mq.kafka.model.MQModel;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import java.util.UUID;
//
//@Component
//public class SketcherAnalysisReportProducer {
//
//    @Value("${spring.kafka.producer.topic.sketcher-analysis-report}")
//    private String testTopic;
//    @Value("${spring.kafka.producer.topic.sketcher-analysis-report2}")
//    private String producerTopic2;
//
//    private MQClient mqClient = MQClientFactory.getInstance().getMQClient(MqType.KAFKA);
//
//    public void send() {
//
//
//        //todo demo
//        String msgId = UUID.randomUUID().toString();
//        MQModel mqModel = MQModel.builder()
//            .age("1")
//            .id(msgId).name("xl").six("男呗")
//            .build();
//        mqClient.publish(testTopic, msgId, JsonUtil.toJson(mqModel));
//        mqClient.publish(producerTopic2,msgId, JsonUtil.toJson(mqModel));
//
//
//    }
//
//
//}
