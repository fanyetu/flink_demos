package cn.fanyetu.flink.main;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HotItems {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 方便测试，设置并行度为1
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\workspace\\flink_demos\\userBehaviorAnalysis\\Data\\UserBehavior.csv");




    }
}
