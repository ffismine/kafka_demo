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
        //程序的唯一标识符以区别于其他应用程序与同一Kafka集群通信
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //用于建立与Kafka集群的初始连接的主机/端口对的列表
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, myProducer.bootstrap_servers);
        //记录键值对的默认序列化和反序列化库
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //定义Streams应用程序的计算逻辑,计算逻辑被定义为topology连接的处理器节点之一
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(myProducer.TOPIC1).to(myProducer.TOPIC2);
        //构建Topology对象
        final Topology topology = builder.build();

        System.out.println(topology.describe());
        //构建 kafka流 API实例
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        // 附加关闭处理程序来捕获control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
