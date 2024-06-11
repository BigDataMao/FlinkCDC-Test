package cn.chatdoge.flink117.source;

import cn.chatdoge.flink117.utils.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Description: 自定义的数据源
 * 该方法并行度必须为1
 * @Author: Simon Mau
 * @Date: 2023/11/27 16:32
 */
public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    String[] ids = {"Alice", "Bob", "Cindy", "David"};
    String[] urls = {"www.baidu.com", "www.google.com", "www.bing.com", "www.360.com"};

    Random random = new Random();

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running) {
            String id = ids[random.nextInt(ids.length)];
            Long ts = System.currentTimeMillis();
            String url = urls[random.nextInt(urls.length)];
            ctx.collect(new Event(id, ts, url));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
