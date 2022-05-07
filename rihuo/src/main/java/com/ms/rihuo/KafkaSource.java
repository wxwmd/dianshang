package com.ms.rihuo;

import com.alibaba.fastjson.JSONObject;
import com.ms.dianshang.bean.LoginLog;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","192.168.126.128:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"wxwmd");

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList("gmall_start_0523"));


        while(true){
            ConsumerRecords<String,String> callLogs=kafkaConsumer.poll(100);
            for (ConsumerRecord record:callLogs){
                LoginLog loginLog = JSONObject.parseObject((String) record.value(), LoginLog.class);
                System.out.println(loginLog.toString());
            }
        }
    }
}
