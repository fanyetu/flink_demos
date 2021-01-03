package cn.fanyetu.flink_demos.shizhan01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyNoParalleSource implements SourceFunction<String> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            //图书的排行榜
            List<String> books = new ArrayList<>();
            books.add("Python从入门到放弃");//10
            books.add("Java从入门到放弃");//8
            books.add("Php从入门到放弃");//5
            books.add("C++从入门到放弃");//3
            books.add("Scala从入门到放弃");
            int i = new Random().nextInt(5);
            String book = books.get(i);
            logger.info("写入boot: {}", book);
            ctx.collect(book);
            //每2秒产生一条数据
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
