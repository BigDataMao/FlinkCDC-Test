package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Samuel Mau
 */
public class ConnectorKafka {
    public static void main(String[] args) {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为kafka
        tableEnv.executeSql("""
                CREATE TABLE kafkaSource (
                  id INT,
                  name STRING
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'test',
                  'properties.bootstrap.servers' = 'localhost:9092',
                  'format' = 'json',
                  'scan.startup.mode' = 'earliest-offset'
                )""");

        // 创建sink表,写入kafka
        tableEnv.executeSql("""
                CREATE TABLE kafkaSink (
                  id INT,
                  name STRING
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'testSink',
                  'properties.bootstrap.servers' = 'localhost:9092',
                  'format' = 'json',
                  'sink.partitioner' = 'round-robin'
                )""");

        // 执行sink
        tableEnv.executeSql("INSERT INTO kafkaSink SELECT id, name FROM kafkaSource WHERE id = 1");
    }

}
