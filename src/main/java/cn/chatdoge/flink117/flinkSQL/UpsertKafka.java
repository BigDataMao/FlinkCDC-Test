package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpsertKafka {
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


        // 创建sink表,用于写入kafka,有两个字段id和cnt,其中id为主键,cnt为id的计数
        tableEnv.executeSql("CREATE TABLE kafkaSink (\n" +
                "  id INT,\n" +
                "  cnt BIGINT,\n" +
                "  primary key (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'upsert-kafka',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");


        // 执行sink
        tableEnv.executeSql("INSERT INTO kafkaSink " +
                "SELECT \n" +
                    "id, \n" +
                    "COUNT(*) \n" +
                "FROM kafkaSource \n" +
                "GROUP BY id"
        );


    }
}
