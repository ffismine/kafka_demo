package kafka_demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class myConsumer {

    private final KafkaConsumer<String, String> consumer;
    public final static String TOPIC = myProducer.TOPIC2;

    public myConsumer(String groupID) {
        Properties props = new Properties();

        props.put("bootstrap.servers", myProducer.bootstrap_servers);
        props.put("group.id", groupID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

     void consume(){
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }


    public static void main (String[] args){
        // 记得修改groupID
        myConsumer Consumer = new myConsumer("GROUPID__01");
        Consumer.consume();
    }

}

