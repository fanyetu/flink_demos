package cn.fanyetu.flink.main;

import cn.fanyetu.flink.pojo.ItemViewCount;
import cn.fanyetu.flink.pojo.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 方便测试，设置并行度为1
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\workspace\\flink_demos\\userBehaviorAnalysis\\Data\\UserBehavior.csv");

        // 转换pojo，并分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                    }
                })
                // 因为这里的数据是已经经过elt的，所以数据是有序的，直接使用AscendingTimestampExtractor提取时间即可。即waterMark = 0
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000;
                    }
                });

        // 分组开窗聚合，得到每个窗口内，各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                // 过滤pv行为
                .filter(data -> "pv".equals(data.getBehavior()))
                // 按商品id分组
                .keyBy("itemId")
                // 开启滑动窗口，窗口大小1小时，每5分钟聚合一次
                .timeWindow(Time.hours(1), Time.minutes(5))
                // 进行聚合
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 收集同一窗口的所有商品count数据，然后排序输出topN
        DataStream<String> resultStream = windowAggStream
                // 按window分组
                .keyBy("windowEnd")
                // 用自定义处理函数，排序取前五
                .process(new TopNHotItems(5));

        resultStream.print();

        env.execute("hotItems");
    }

    // 实现自定义的processFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private Integer top;

        public TopNHotItems(Integer top) {
            this.top = top;
        }

        // 定义列表状态，保存当前窗口内所有的输出的itemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        // 在open生命周期中创建这个状态
        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据， 存入list，并注册定时器
            itemViewCountListState.add(value);
            // 因为一个window中所有数据的windowEnd都是一样的，所以只注册了一个定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        // 定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 当前已拿到所有数据，输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount() - o1.getCount();
                }
            });

            // 输出string
            StringBuilder sb = new StringBuilder();
            sb.append("---------------------\n")
                    .append("窗口结束时间：")
                    .append(timestamp - 1)
                    .append("\n");
            for (int i = 0; i < Math.min(top, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                sb.append("No ").append(i + 1)
                        .append(":").append(itemViewCount.getItemId())
                        .append(",").append(itemViewCount.getCount())
                        .append("\n");
            }
            sb.append("-----------------------");

            Thread.sleep(1000);
            out.collect(sb.toString());
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            // 因为已经经过累加器了，所以这里只有一个数据
            Long count = input.iterator().next();

            out.collect(new ItemViewCount(itemId, count.intValue(), windowEnd));
        }
    }

    // 实现自定义的增量聚合函数，AggregateFunction没法拿到Window信息，所以这里输出就是总和
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        // 定义累加器初始值
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 累加，定义累加器如何基于输入数据进行累加
        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        // 输出
        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        // 合并，定义累加器如何和State中的累加器进行合并
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
