package cn.chatdoge.fink113.sinkAPI;

import cn.chatdoge.fink113.source.ClickSource;
import cn.chatdoge.fink113.utils.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 用flinkSql语言写入MySQL, 仅插入
 * @Author: Simon Mau
 * @Date: 2023/12/1 17:55
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDS = env.addSource(new ClickSource());

        SinkFunction<Event> mySink = JdbcSink.sink(
                "INSERT INTO test.mao (id, ts, url) VALUES (?, ?, ?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getId());
                        preparedStatement.setLong(2, event.getTs());
                        preparedStatement.setString(3, event.getUrl());
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://txy:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("mxw19910712@MYSQL")
                        .build()
        );

        eventDS.addSink(mySink);

        env.execute();
    }

}
