package cn.chatdoge.finkCDC.source;

import cn.chatdoge.finkCDC.utils.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: 将自定义数据源跑起来
 * @Author: Simon Mau
 * @Date: 2023/11/27 16:56
 */
public class ClickSourceRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> customSource01 = env.addSource(new ClickSource());
//        DataStreamSource<Event> customSource02 = env.addSource(new ParallelClickSource()).setParallelism(2);
        customSource01.print();

        env.execute("test custom source");
    }

}
