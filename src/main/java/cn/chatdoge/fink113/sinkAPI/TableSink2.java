package cn.chatdoge.fink113.sinkAPI;

import cn.chatdoge.fink113.source.ClickSource;
import cn.chatdoge.fink113.utils.Event;
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

/**
 * @Description table -> changelogStream 支持upsert
 * 要求：MySQL表中必须有主键,changelogStream进行sink的时候需先将数据进行摘取,只取部分含数据的字段
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



        String sql = "INSERT INTO test.idCount (id, idCount) VALUES (?, ?) "
                + "ON DUPLICATE KEY UPDATE idCount = VALUES(idCount)";

        // 该写法比较取巧,仅取changelogStream中的第一个字段和第二个字段,抛弃了flag(+U,-U).并不影响upsert
        JdbcStatementBuilder<Row> jdbcStatementBuilder = (preparedStatement, row) -> {
            String field_0 = (String) row.getField(0);
            Integer field_1 = (Integer) row.getField(1);
            preparedStatement.setString(1, field_0);
            //preparedStatement.setInt(2, field_1);  这样写会报错,Integer拆箱成int,如果null会报错,null不能拆箱
            preparedStatement.setInt(2, field_1 == null ? 0 : field_1);
        };

        JdbcExecutionOptions jdbcExecutionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1)
                .build();


        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://txy:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("mxw19910712@MYSQL")
                .build();

        SinkFunction<Row> sinkFunction = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                jdbcExecutionOptions,
                jdbcConnectionOptions
        );

        changelogStream.addSink(sinkFunction);

        env.execute();
    }
}
