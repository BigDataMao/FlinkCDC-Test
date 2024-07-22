package cn.chatdoge.flink117.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpsertMySQL {
    public static void main(String[] args) {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 接入dataGen数据源
        tableEnv.executeSql("CREATE TABLE dataGen (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second'='1',\n" +
                "  'fields.id.kind'='random',\n" +
                "  'fields.id.min'='1',\n" +
                "  'fields.id.max'='10',\n" +
                "  'fields.name.length'='10'\n" +
                ")");

        // 创建sink表,写入mysql
        tableEnv.executeSql("CREATE TABLE mysqlSink (\n" +
                "  id INT,\n" +
                "  cnt BigInt,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "  'table-name' = 'idCnt',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456'\n" +
                ")");

        // 执行写入
        tableEnv.executeSql("INSERT INTO mysqlSink " +
                "SELECT \n" +
                    "id, \n" +
                    "COUNT(*) as cnt \n" +
                "FROM dataGen \n" +
                "GROUP BY id"
        );
    }
}
