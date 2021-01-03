package cn.fanyetu.flink_demos.shizhan01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置精确一次的检查点
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 启用检查点
        env.enableCheckpointing(5000);

        // 添加数据源，并设置并行度为1
        DataStreamSource<String> ds = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.152.128:9092");

        // 2.0 配置 KafkaProducer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "192.168.152.128:9092", //broker 列表
                "test",           //topic
                new SimpleStringSchema()); // 消息序列化

//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("test",
//                new KafkaSerializationSchema<String>() {
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                        return new ProducerRecord<>("test", element.getBytes(StandardCharsets.UTF_8));
//                    }
//                },
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // 写入kafka时携带时间戳
        producer.setWriteTimestampToKafka(true);
        // 添加sink
        ds.addSink(producer);
        // 执行
        env.execute();

    }
}
