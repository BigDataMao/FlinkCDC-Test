package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SimpleExample {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为dataGen
        tableEnv.executeSql("CREATE TABLE dataGen (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second'='1',\n" +
                // 如果是序列生成器，需要指定序列的起始值和结束值
//                "  'fields.id.kind'='sequence',\n" +
//                "  'fields.id.start'='1',\n" +
//                "  'fields.id.end'='100',\n" +
                // 如果是随机生成器，需要指定随机数的范围
                "  'fields.id.kind'='random',\n" +
                "  'fields.id.min'='1',\n" +
                "  'fields.id.max'='10',\n" +
                // 字符串长度
                "  'fields.name.length'='10'\n" +
                ")");

        Table myTable = tableEnv.sqlQuery("SELECT id FROM dataGen");

        // 注册表
        tableEnv.createTemporaryView("myTable", myTable);
        tableEnv.executeSql("SELECT * FROM myTable").print();

//        // 将表写入kafka
//        tableEnv.executeSql("CREATE TABLE kafkaSink (\n" +
//                "  id INT,\n" +
//                "  name STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'test',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'format' = 'json'\n" +
//                ")");
//        tableEnv.executeSql("INSERT INTO kafkaSink SELECT * FROM dataGen");



        // 执行作业
//        env.execute();  // 如果是纯table api，不需要执行env.execute()
    }
}
