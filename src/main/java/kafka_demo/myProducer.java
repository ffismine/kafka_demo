package kafka_demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class myProducer{

    private final Producer<String, String> producer;
    public final static String TOPIC1 = "topic_1";
    public final static String TOPIC2 = "topic_2";
    public final static String bootstrap_servers = "xx.xx.xxx.xx:xxxx";

    //构造方法
    private myProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        producer = new KafkaProducer<String, String>(props);
    }

    void produce() {
        for (int i = 0; i < 100; i++) {
            String msg = "Message__" + i;
            producer.send(new ProducerRecord<String, String>(TOPIC1, msg));
            System.out.println("Sent:" + msg);
        }
    }

    public static void main(String[] args) {
        myProducer Producer = new myProducer();
        Producer.produce();
    }
}