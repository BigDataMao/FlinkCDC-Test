package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SimpleExample {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为dataGen
        tableEnv.executeSql("""
                CREATE TABLE dataGen (
                  id INT,
                  name STRING
                ) WITH (
                  'connector' = 'datagen',
                  'rows-per-second'='1',
                  'fields.id.kind'='random',
                  'fields.id.min'='1',
                  'fields.id.max'='10',
                  'fields.name.length'='10'
                )""");

        // 将表写入kafka
        tableEnv.executeSql("""
                CREATE TABLE kafkaSink (
                  id INT,
                  name STRING
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'test',
                  'properties.bootstrap.servers' = 'localhost:9092',
                  'format' = 'json'
                )""");
        tableEnv.executeSql("INSERT INTO kafkaSink SELECT * FROM dataGen");

    }
}
