package cn.chatdoge.flink117.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Samuel Mau
 * @description 用来测试滚动时间窗口,结合watermark
 * 运行的时候需要跟GenerateOutOfOrderDataStream一起运行
 * 这样才能实时观察每条数据在什么时候触发窗口计算
 */

public class TimeWindowExample {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为kafka
        tableEnv.executeSql("""
                CREATE TABLE kafkaSource (
                  id INT,
                  name STRING,
                  event_time TIMESTAMP(3),
                  -- watermark
                  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
                ) WITH (
                  'connector' = 'kafka',
                    'topic' = 'outOfOrderDataStream',
                    'properties.bootstrap.servers' = 'localhost:9092',
                    'format' = 'json',
                    'scan.startup.mode' = 'earliest-offset'
                )""");

        // 创建用于打印的表 (临时表)
        tableEnv.executeSql("""
                CREATE TABLE printTable (
                  id INT,
                  name STRING,
                  event_time TIMESTAMP(3)
                ) WITH (
                  'connector' = 'print'
                )""");

        // 创建滚动时间窗口,每5秒统计一次cnt值,connector为print
        tableEnv.executeSql("""
                CREATE TABLE timeWindowTable (
                  cnt BIGINT,
                  window_start TIMESTAMP(3),
                  window_end TIMESTAMP(3)
                ) WITH (
                  'connector' = 'print'
                )""");

        // 将每条数据插入到临时打印表中
        tableEnv.executeSql("""
                INSERT INTO printTable
                SELECT id, name, event_time
                FROM kafkaSource
                """);

        // 执行窗口统计
        tableEnv.executeSql("""
                INSERT INTO timeWindowTable
                SELECT
                    COUNT(*),
                    TUMBLE_START(event_time, INTERVAL '5' SECOND),
                    TUMBLE_END(event_time, INTERVAL '5' SECOND)
                FROM kafkaSource
                GROUP BY TUMBLE(event_time, INTERVAL '5' SECOND)
                """);


    }
}
