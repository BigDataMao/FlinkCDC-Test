package cn.chatdoge.flink113.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Description 能设置并行度的自定义数据源方法
 * @Author simon.mau
 * @Date 2023/11/27 21:55
 */
public class ParallelClickSource implements ParallelSourceFunction<Integer> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            Random random = new Random();
            sourceContext.collect(random.nextInt(100));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
