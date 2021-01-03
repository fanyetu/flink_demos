package cn.fanyetu.flink_demos.shizhan02;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class MockOrderSource implements SourceFunction<String> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            int i = new Random().nextInt(10);
            double p = new Random().nextDouble() * 100;
            OrderDetail orderDetail = new OrderDetail();
            orderDetail.setCiteName("CQ");
            orderDetail.setItemId((long) i);
            orderDetail.setPrice(p);
            orderDetail.setUserId((long) i);
            orderDetail.setTimeStamp(System.currentTimeMillis());

            String json = JSON.toJSONString(orderDetail);
            logger.info("生产数据：" + json);
            ctx.collect(json);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
