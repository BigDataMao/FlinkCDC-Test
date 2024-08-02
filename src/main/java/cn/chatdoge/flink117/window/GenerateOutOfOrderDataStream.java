package cn.chatdoge.flink117.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GenerateOutOfOrderDataStream {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为datagen
        tableEnv.executeSql("""
                CREATE TABLE dataGen (
                  id INT,
                  name STRING,
                  event_time TIMESTAMP_LTZ(3)
                ) WITH (
                  'connector' = 'datagen',
                  'rows-per-second'='1',
                  'fields.id.kind'='sequence',
                  'fields.id.start'='1',
                  'fields.id.end'='100',
                  'fields.name.length'='5'
                )""");

        // 制造1秒乱序效果的数据
        tableEnv.executeSql("""
                CREATE TABLE outOfOrderDataStream (
                  id INT,
                  name STRING,
                  event_time TIMESTAMP_LTZ(3)
                ) WITH (
                  'connector' = 'kafka',
                    'topic' = 'outOfOrderDataStream',
                    'properties.bootstrap.servers' = 'localhost:9092',
                    'format' = 'json'
                )""");

        // 插入数据,并将event_time设置为乱序
        tableEnv.executeSql("""
                INSERT INTO outOfOrderDataStream
                SELECT
                    id,
                    name,
                    event_time + INTERVAL '1' SECOND * CAST(FLOOR(RAND() * 5) - 1 AS INT) AS event_time
                FROM dataGen
                """);

    }
}
