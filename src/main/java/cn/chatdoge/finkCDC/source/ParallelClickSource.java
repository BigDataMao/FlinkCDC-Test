package cn.chatdoge.finkCDC.source;

import cn.chatdoge.finkCDC.utils.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Description 能设置并行度的自定义数据源方法
 * @Author simon.mau
 * @Date 2023/11/27 21:55
 */
public class ParallelClickSource implements ParallelSourceFunction<Event> {
    private boolean running = true;
    String[] ids = {"Alice", "Bob", "Cindy", "David"};
    String[] urls = {"www.baidu.com", "www.google.com", "www.bing.com", "www.360.com"};

    Random random = new Random();
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running) {
            String id = ids[random.nextInt(ids.length)];
            Long ts = System.currentTimeMillis();
            String url = urls[random.nextInt(urls.length)];
            sourceContext.collect(new Event(id, ts, url));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
