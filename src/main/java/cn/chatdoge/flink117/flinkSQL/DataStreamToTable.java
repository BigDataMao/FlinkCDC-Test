package cn.chatdoge.flink117.flinkSQL;

import cn.chatdoge.flink117.POJO.IdCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamToTable {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建一个DS,数据源为kafka
        // FlinkKafkaConsumer 已被弃用并将在 Flink 1.17 中移除，请改用 KafkaSource
        KafkaSource<IdCount> stringKafkaSource = KafkaSource.<IdCount>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(IdCount.class))
                .build();

        DataStreamSource<IdCount> dsSource = env.fromSource(
                stringKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );
        // 将DS注册为表
        tableEnv.createTemporaryView("kafkaTable", dsSource);
        // 执行查询
        tableEnv.executeSql("select id from kafkaTable").print();


        env.execute();

    }
}
