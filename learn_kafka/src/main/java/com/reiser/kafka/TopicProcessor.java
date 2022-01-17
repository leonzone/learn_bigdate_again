package com.reiser.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author: reiserx
 * Date:2021/10/18
 * Des: 创建主题
 */
public class TopicProcessor extends Thread {

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public TopicProcessor(String topic, Boolean isAsync) {
        Properties props = new Properties();
        // 用户拉取kafka的元数据
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("client.id", "DemoProducer");
        //设置序列化的类。
        // 二进制的格式
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //消费者，消费数据的时候，就需要进行反序列化。
        // 初始化kafkaProducer
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    @Override
    public void run() {
        int messageNo = 1;
        // 一直会往kafka发送数据
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            //isAsync , kafka发送数据的时候，有两种方式
            // 1: 异步发送
            // 2: 同步发送
            // isAsync: true的时候是异步发送，false就是同步发送
            if (isAsync) {
                // Send asynchronously
                // 异步发送
                // 这样的方式，性能比较好，我们生产代码用的就是这种方式。
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    //同步发送
                    // 发送一条消息，等这条消息所有的后续工作都完成以后才继续下一条消息的发 送。
                    producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ++messageNo;

        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (exception != null) {
            System.out.println("有异常");
            //一般我们生产里面 还会有其它的备用的链路。

        } else {
            System.out.println("说明没有异常信息，成功的！！");

        }

        if (metadata != null) {
            System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");

        } else {
            exception.printStackTrace();

        }

    }

    public static void main(String[] args) {
        TopicProcessor producer = new TopicProcessor("topic001", Boolean.TRUE);
        producer.start();
    }
}
