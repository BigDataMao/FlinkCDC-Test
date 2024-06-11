package cn.chatdoge.flink117.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Description: 接Kafka数据源
 * @Author: Simon Mau
 * @Date: 2024/6/7 11:11
 */
public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka连接参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "cdh-node2:9092");
        prop.setProperty("group.id", "flink");  // 消费者组

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flume-kafka-flink",
                new SimpleStringSchema(),
                prop);

        // 添加数据源
        env.addSource(kafkaConsumer).print();

        // 执行任务
        env.execute("kafka source");
    }

}
