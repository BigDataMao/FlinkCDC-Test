package cn.chatdoge.fink113.source;

import cn.chatdoge.fink113.utils.EventOnlyIdAndUrl;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Description
 * @Author simon.mau
 * @Date 2023/12/3 22:40
 */
public class ClickSource2 implements SourceFunction<EventOnlyIdAndUrl>{
    private boolean running = true;
    String[] ids = {"Alice", "Bob", "Cindy", "David"};
    String[] urls = {"www.baidu.com", "www.google.com", "www.bing.com", "www.360.com"};

    Random random = new Random();

    @Override
    public void run(SourceFunction.SourceContext<EventOnlyIdAndUrl> ctx) throws Exception {
        while (running) {
            String id = ids[random.nextInt(ids.length)];
            Long ts = System.currentTimeMillis();
            String url = urls[random.nextInt(urls.length)];
            ctx.collect(new EventOnlyIdAndUrl(id, url));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
