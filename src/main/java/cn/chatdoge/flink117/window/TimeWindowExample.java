package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.IdCount;
import oracle.ucp.common.waitfreepool.Tuple;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;

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
        DataStream<IdCount> dataStream = tableEnv.toDataStream(table, IdCount.class).map(
                t -> new IdCount(t.getId(), t.getName())
        );

//        // 转化为DataStream
//        DataStream<Tuple2<Integer, String>> dataStream = tableEnv.toDataStream(table, IdCount.class)
//                .map(t -> Tuple2.of(t.getId(), t.getName()))
//                .returns(Types.TUPLE(Types.INT, Types.STRING));

        // 按照id分组，每5秒统计一次
        dataStream.countWindowAll(3)
                .max("id")
                .print();

        env.execute();
    }
}
