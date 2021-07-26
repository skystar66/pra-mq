package com.bizseer.hubble.mq.kafka;

import com.bizseer.hubble.mq.MQClientFactory;
import com.bizseer.hubble.mq.enums.MqType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author fyt
 * @date 2021/7/13
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class KafkaClientUtil {

    public static void produce(String topic, String msgId, String value) {
        MQClientFactory.getInstance().getMQClient(MqType.KAFKA).publish(topic, msgId, value);
    }

    public static boolean produceAsync(String topic, String msgId, String value) {
        return MQClientFactory.getInstance().getMQClient(MqType.KAFKA).publish(topic, msgId, value).isDone();
    }


}
