package cn.chatdoge.flink117.window;

import cn.chatdoge.flink117.POJO.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;

public class TimeWindowExample {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建表,数据源为datagen
        tableEnv.executeSql(
                "CREATE TABLE dataGen (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  eventTime TIMESTAMP\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second'='1',\n" +
                "  'fields.id.kind'='sequence',\n" +
                "  'fields.id.start'='1',\n" +
                "  'fields.id.end'='100',\n" +
                "  'fields.name.length'='5'\n" +
                ")");

        Table table = tableEnv.sqlQuery(
                "SELECT " +
                        "id, " +
                        "name, " +
                        "TIMESTAMPADD(HOUR, 8, eventTime) ts " +
                   "FROM dataGen"
        );

        DataStream<Order> dataStream = tableEnv.toDataStream(table, Order.class).map(
                t -> {
                    long seconds = t.getTs().getTime() / 1000;
                    Timestamp ts = new Timestamp(seconds * 1000);
                    return new Order(t.getId(), t.getName(), ts);
                }
        );

        dataStream.print();


        env.execute();
    }
}
