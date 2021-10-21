package com.reiser.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author: reiserx
 * Date:2021/10/18
 * Des: 消费者消费消息
 */
public class KafkaConsumerDemo {
    public static final String BROKERS = "reiser001:9092,reiser002:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        // 定义 kakfa 服务的地址，不需要将所有 broker 指定上
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        // key 的序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // value 的序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("server_log_v30"));

        while (true) {
            // 读取数据，读取超时时间为 100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value= %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
