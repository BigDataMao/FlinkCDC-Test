package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;

public class TimeWindowExample {
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
                  event_time TIMESTAMP(3),
                  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
                ) WITH (
                  'connector' = 'datagen',
                  'rows-per-second'='2',
                  'fields.id.kind'='sequence',
                  'fields.id.start'='1',
                  'fields.id.end'='100',
                  'fields.name.length'='5'
                )""");

        tableEnv.executeSql("""
                        CREATE VIEW orderCnt as
                        SELECT
                           COUNT(*) AS cnt,
                           TUMBLE_START(event_time, INTERVAL '5' SECOND) AS window_start,
                           TUMBLE_END(event_time, INTERVAL '5' SECOND) AS window_end
                        FROM dataGen
                           GROUP BY TUMBLE(event_time, INTERVAL '5' SECOND)
                """);

        tableEnv.executeSql("SELECT * FROM orderCnt").print();


        env.execute();
    }
}
