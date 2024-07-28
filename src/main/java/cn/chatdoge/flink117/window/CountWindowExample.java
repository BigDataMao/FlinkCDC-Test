package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.IdName;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CountWindowExample {
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
        DataStream<IdName> dataStream = tableEnv.toDataStream(table, IdName.class)
                // 必须执行map操作，否则flink无法识别为正确的POJO
                .map(
                        t -> new IdName(t.getId(), t.getName())
                );

        // 按照id分组，每5秒统计一次
        dataStream.countWindowAll(3)
                .max("id")
                .print();

        env.execute();
    }
}
