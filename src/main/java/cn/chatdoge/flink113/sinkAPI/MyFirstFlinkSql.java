package cn.chatdoge.flink113.sinkAPI;

import cn.chatdoge.flink113.source.ClickSource;
import cn.chatdoge.flink113.utils.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.call;



import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: 我的第一个flinkSql脚本测试
 * @Author: Simon Mau
 * @Date: 2023/12/1 14:03
 */
public class MyFirstFlinkSql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 创建自定义数据源
        DataStreamSource<Event> eventDS = env.addSource(new ClickSource());

//        // TODO 01. DataStream -> View
//        tableEnv.createTemporaryView("clicks", eventDS);
//        // 筛选表中数据
//        String sql = "select id,from_unixTime(ts/1000) as ts from clicks where id = 'Alice'";
//        tableEnv.executeSql(sql).print();

        // TODO 02. DataStream -> Table
        Table clickTable = tableEnv.fromDataStream(eventDS);
        // 筛选表中数据
        Table selectedTable = clickTable.select(
                $("id"),
                $("ts")
        );
//        selectedTable.execute().print();

        // TODO 03. Table -> View
        tableEnv.createTemporaryView("clicks", selectedTable);
        tableEnv.executeSql("select id,count(id) as idCount from clicks group by id").print();

    }
}
