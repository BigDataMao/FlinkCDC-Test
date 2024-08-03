package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.Order;
import cn.chatdoge.flink117.deserializationSchema.CustomOrderDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Samuel Mau
 * @description 同TimeWindowSQL.java, 但是使用DataStream API(很少使用,且很难,除非业务需要自定义实现)
 * 本次只测试实现自定义的AggregateFunction
 */
public class TimeWindowAggregateFunction {
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
                .aggregate(new MyIdCollectAggFunction())
                .print();

        env.execute();
    }

    private static class MyIdCollectAggFunction implements AggregateFunction<Order, StringBuilder, String> {

        @Override
        public StringBuilder createAccumulator() {
            return new StringBuilder("本次窗口所有id: ");
        }

        @Override
        public StringBuilder add(Order value, StringBuilder accumulator) {
            System.out.println(value.toString());
            return accumulator.append(value.getId()).append(" ");
        }

        @Override
        public String getResult(StringBuilder accumulator) {
            return accumulator.toString();
        }

        @Override
        public StringBuilder merge(StringBuilder a, StringBuilder b) {
            return a.append(b);
        }
    }
}
