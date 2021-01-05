package cn.fanyetu.flink_demos.shizhan02;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.*;

public class TopNCalculator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置flink的时间特征为event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置checkpoint周期为60秒
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.152.128:9092");
        properties.setProperty("group.id", "test_consumer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test01", new SimpleStringSchema(), properties);
        // 设置从最开始进行消费
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple2<Double, OrderDetail>> process = stream.map(message -> JSON.parseObject(message, OrderDetail.class))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {
                    private Long currentTimeStamp = 0L;
                    //设置允许乱序时间
                    private Long maxOutOfOrderness = 5000L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
                        return element.getTimeStamp();
                    }
                })
                .keyBy((KeySelector<OrderDetail, ? extends Object>) OrderDetail::getUserId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(
                                value1.getUserId(), value1.getItemId(), value1.getCiteName(),
                                value1.getPrice() + value2.getPrice(), value1.getTimeStamp()
                        );
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                        TreeMap<Double, OrderDetail> treeMap = new TreeMap<>(new Comparator<Double>() {
                            @Override
                            public int compare(Double x, Double y) {
                                return (x < y) ? -1 : 1;
                            }
                        });
                        Iterator<OrderDetail> iterator = elements.iterator();
                        if (iterator.hasNext()) {
                            treeMap.put(iterator.next().getPrice(), iterator.next());
                            if (treeMap.size() > 10) {
                                treeMap.pollLastEntry();
                            }
                        }
                        for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                        }
                    }
                });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.152.128").setPort(6379).build();
        process.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<Double, OrderDetail>>() {
            private final String TOPN_PREFIX = "TOPN:";
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,TOPN_PREFIX);
            }
            @Override
            public String getKeyFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f0);
            }
            @Override
            public String getValueFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f1.toString());
            }
        }));

        env.execute();

    }
}
