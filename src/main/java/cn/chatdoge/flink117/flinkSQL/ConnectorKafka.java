package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        tableEnv.executeSql("CREATE TABLE kafkaSource (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        // 打印数据
        tableEnv.executeSql("SELECT * FROM kafkaSource").print();


    }

}
