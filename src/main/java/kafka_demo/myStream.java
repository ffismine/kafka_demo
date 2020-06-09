package kafka_demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class myStream {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, myProducer.bootstrap_servers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(myProducer.TOPIC1).to(myProducer.TOPIC2);
        //构建Topology对象
        final Topology topology = builder.build();

        System.out.println(topology.describe());
        //构建 kafka流 API实例
        final KafkaStreams streams = new KafkaStreams(topology, props);

        try {
            streams.start();
            streams.close();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
