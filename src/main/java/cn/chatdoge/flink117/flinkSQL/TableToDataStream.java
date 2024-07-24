package cn.chatdoge.flink117.flinkSQL;

import cn.chatdoge.flink117.POJO.IdCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableToDataStream {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为datagen
        TableResult tableResult = tableEnv.executeSql("CREATE TABLE dataGen (\n" +
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
        tableEnv.toDataStream(table, IdCount.class).print();

        env.execute();

    }
}
