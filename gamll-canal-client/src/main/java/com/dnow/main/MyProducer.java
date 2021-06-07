package com.dnow.main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    private static Producer<String,String> producer;

    static {
        producer = getKafkaProducer();
    }

    public static Producer<String,String> getKafkaProducer(){
        Properties properties = new Properties();
        // 必须配置的三个参数
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);
    }

    // 向kafka写入数据
    public static void writeToKafka(String topic, String value){
        producer.send(new ProducerRecord<String,String>(topic,value));
    }
}
