//package com.bizseer.mq.kafka.init;
//
//import com.bizseer.hubble.mq.MQClient;
//import com.bizseer.hubble.mq.MQClientFactory;
//import com.bizseer.hubble.mq.enums.MqType;
//import com.bizseer.mq.kafka.consumer.MQTestModelConsumer;
//import com.bizseer.mq.kafka.consumer.MQTestStrConsumer;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.PreDestroy;
//import javax.annotation.Resource;
//
//
//@Component
//public class KafkaMQClientinitializer {
//
//    //todo 会变动
//    @Value("${spring.kafka.consumer.topic.sketcher-analysis-event}")
//    private String testTopic;
//    @Value("${spring.kafka.consumer.group.sketcher-analysis-group}")
//    private String testGroupTopic;
//    @Value("${spring.kafka.consumer.topic.sketcher-analysis-report2}")
//    private String testTopic2;
//
//    @Resource
//    MQTestModelConsumer mqTestModelConsumer;
//    @Resource
//    MQTestStrConsumer mqTestStrConsumer;
//
//
//    private MQClient mqClient = MQClientFactory.getInstance().getMQClient(MqType.KAFKA);
//
//    /**
//     * 初始化{配置+subTopic}
//     *
//     * @param
//     * @return: void
//     * @author: xl
//     * @date: 2021/7/8
//     **/
//
//    @PostConstruct
//    public void init() {
//        mqClient.subscribe(testGroupTopic, testTopic, mqTestModelConsumer);
//        mqClient.subscribe(testGroupTopic, testTopic2, mqTestStrConsumer);
//    }
//
//    @PreDestroy
//    public void destroy() {
//        mqClient.destory();
//    }
//}
