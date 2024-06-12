package cn.chatdoge.flink117.source;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

/**
 * @Description: 接Kafka数据源
 * @Author: Simon Mau
 * @Date: 2024/6/7 11:11
 */
public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);
        // using batch mode for bounded data
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // kafka连接参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "cdh-node2:9092");
        prop.setProperty("group.id", "flink");  // 消费者组

        // 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(prop)
                .setTopics("flume-kafka-flink")
                .setDeserializer(new CustomKafkaDeserializationSchema())
                .build();

        // 添加数据源
        DataStream<RowData> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source")
                .map((MapFunction<String, RowData>) value -> {
                    // 打印从kafka中读取的数据
                    System.out.println("flink获得的value: " + value);

                    String uuid = UUID.randomUUID().toString();
                    TimestampData dt = TimestampData.fromInstant(Instant.now());
                    GenericRowData genericRowData = new GenericRowData(3);
                    genericRowData.setField(0, StringData.fromString(uuid));
                    genericRowData.setField(1, dt);
                    genericRowData.setField(2, StringData.fromString(value));

                    // 打印构造的RowData
                    System.out.println("flink重组的RowData: " + genericRowData);

                    return genericRowData;
                });

        // 添加doris连接参数
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("cdh-master:8036")
                .setTableIdentifier("flink.test")
                .setUsername("admin")
                .setPassword("BJcgSGk4(icB1Sczb")
                .build();

        // 添加doris执行参数
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris")
                .setDeletable(false)
                .setStreamLoadProp(properties);

        // 添加doris sink
        String[] fieldNames = {"uuid", "dt", "word"};
        DataType[] types = {
                DataTypes.VARCHAR(255),
                DataTypes.TIMESTAMP(3),
                DataTypes.STRING()
        };

        DorisSink.Builder<RowData> dorisBuilder = DorisSink.builder();
        dorisBuilder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setType("json")
                        .setFieldType(types)
                        .build())
                .setDorisOptions(dorisOptions);

        dataStream.sinkTo(dorisBuilder.build())
                .name("doris sink")
                .setParallelism(1);

        // 执行任务
        env.execute("kafka source");
    }

    private static class CustomKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<String> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<String> collector) throws IOException {
            String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
            collector.collect(value);

            // 打印从kafka中读取的数据
            System.out.println("kafka反序列化器获得的value: " + value);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

}
