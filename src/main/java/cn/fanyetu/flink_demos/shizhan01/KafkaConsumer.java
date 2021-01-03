package cn.fanyetu.flink_demos.shizhan01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.152.128:9092");
        properties.setProperty("group.id", "group_test");

        // 消费多个topic的数据
        // ArrayList<String> topics = new ArrayList<>();
        // topics.add("test_A");
        // topics.add("test_B");
        //FlinkKafkaConsumer<Tuple2<String, String>> consumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        // 设置从最开始消费topic
        consumer.setStartFromEarliest();
        // 还可以手动指定相应的 topic, partition，offset,然后从指定好的位置开始消费
        // HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
        // map.put(new KafkaTopicPartition("test", 1), 10240L);
        // 假如partition有多个，可以指定每个partition的消费位置
        // map.put(new KafkaTopicPartition("test", 2), 10560L);
        // 然后各个partition从指定位置消费
        // consumer.setStartFromSpecificOffsets(map);

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                System.out.println(s);
            }
        });
        env.execute();
    }
}
