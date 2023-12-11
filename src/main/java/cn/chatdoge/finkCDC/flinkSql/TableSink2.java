package cn.chatdoge.finkCDC.flinkSql;

import cn.chatdoge.finkCDC.source.ClickSource;
import cn.chatdoge.finkCDC.utils.Event;
import cn.chatdoge.finkCDC.utils.IdCount;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description table -> changelogStream 支持upsert
 * 要求：MySQL表中必须有主键,changelogStream进行sink的时候需先将数据进行映射,row -> IdCount
 * @Author simon.mau
 * @Date 2023/12/5 20:25
 */
public class TableSink2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Event> eventDS = env.addSource(new ClickSource());
        tableEnv.createTemporaryView("clicks", eventDS);
        Table table = tableEnv.sqlQuery("select id,count(id) from clicks group by id");
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(table);

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://txy:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("mxw19910712@MYSQL")
                .build();

        JdbcStatementBuilder<Row> jdbcStatementBuilder = new JdbcStatementBuilder<Row>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                preparedStatement.setString(1, row.getField(0).toString());
                preparedStatement.setInt(2, Integer.parseInt(row.getField(1).toString()));
            }
        };

        String sql = "INSERT INTO test.idCount (id, idCount) VALUES (?, ?) "
                + "ON DUPLICATE KEY UPDATE idCount = VALUES(idCount)";

        changelogStream.addSink(JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                jdbcConnectionOptions
        ));

        env.execute();


    }

}
