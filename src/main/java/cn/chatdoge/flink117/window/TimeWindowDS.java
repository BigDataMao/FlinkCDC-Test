package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.Order;
import cn.chatdoge.flink117.deserializationSchema.CustomOrderDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author Samuel Mau
 * @description 同TimeWindowSQL.java, 但是使用DataStream API(很少使用,且很难,除非业务需要自定义实现)
 */
public class TimeWindowDS {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为kafka
        KafkaSource<Order> kafkaSource = KafkaSource.<Order>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("outOfOrderDataStream")
                .setGroupId("idea")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new CustomOrderDeserializationSchema())
                .build();


        DataStreamSource<Order> kafkaSourceWithWaterMark = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((order, ts) -> order.getEvent_time().toInstant().toEpochMilli()),
                "kafkaSource"
        );

        // 执行窗口统计
        kafkaSourceWithWaterMark
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                // .sum("id") // 类似的预制方法还有max,min,然后就没了,都得自己写
                .aggregate(new MyCountAggFunction())
                .print();

//        // 创建用于打印的表 (临时表)
//        tableEnv.executeSql("""
//                CREATE TABLE printTable (
//                  id INT,
//                  name STRING,
//                  event_time TIMESTAMP(3)
//                ) WITH (
//                  'connector' = 'print'
//                )""");
//
//        // 创建滚动时间窗口,每5秒统计一次cnt值,connector为print
//        tableEnv.executeSql("""
//                CREATE TABLE timeWindowTable (
//                  cnt BIGINT,
//                  window_start TIMESTAMP(3),
//                  window_end TIMESTAMP(3)
//                ) WITH (
//                  'connector' = 'print'
//                )""");
//
//        // 将每条数据插入到临时打印表中
//        tableEnv.executeSql("""
//                INSERT INTO printTable
//                SELECT id, name, event_time
//                FROM kafkaSource
//                """);
//
//        // 执行窗口统计
//        tableEnv.executeSql("""
//                INSERT INTO timeWindowTable
//                SELECT
//                    SUM(id),
//                    TUMBLE_START(event_time, INTERVAL '5' SECOND),
//                    TUMBLE_END(event_time, INTERVAL '5' SECOND)
//                FROM kafkaSource
//                GROUP BY TUMBLE(event_time, INTERVAL '5' SECOND)
//                """);

        env.execute();

    }

    private static class MyCountAggFunction implements AggregateFunction<Order, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Order order, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }
}
