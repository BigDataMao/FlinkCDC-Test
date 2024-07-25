package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.IdCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TimeWindowExample {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为datagen
        tableEnv.executeSql("CREATE TABLE dataGen (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second'='1',\n" +
                "  'fields.id.kind'='random',\n" +
                "  'fields.id.min'='1',\n" +
                "  'fields.id.max'='10',\n" +
                "  'fields.name.length'='10'\n" +
                ")");

        Table table = tableEnv.sqlQuery("select * from dataGen");

        // 转化为DataStream
        DataStream<IdCount> dataStream = tableEnv.toDataStream(table, IdCount.class);

        // 创建第二个数据源,这个能正常应用window,上面的不行,因为...
        DataStream<IdCount> dataStream2 = env.fromElements(
                new IdCount(1, "a"),
                new IdCount(2, "b"),
                new IdCount(3, "c"),
                new IdCount(4, "d"),
                new IdCount(5, "e"),
                new IdCount(6, "f"),
                new IdCount(7, "g"),
                new IdCount(8, "h"),
                new IdCount(9, "i"),
                new IdCount(10, "j"),
                new IdCount(11, "a"),
                new IdCount(12, "b")
        );

        // 对于DataStream进行窗口操作
        dataStream
                .countWindowAll(3)
                .max("id")
                .print()
        ;

        env.execute();
    }
}
