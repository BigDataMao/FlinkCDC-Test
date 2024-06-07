package cn.chatdoge.flink113.source;

import cn.chatdoge.flink113.utils.Event;
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
        // 非并行数据源
        DataStreamSource<Event> customSource01 = env.addSource(new ClickSource());
        customSource01.print();
        // 并行数据源
//        DataStreamSource<Integer> customSource02 = env.addSource(new ParallelClickSource()).setParallelism(2);
//        customSource02.print();

        env.execute("test custom source");
    }

}
