package cn.chatdoge.fink113.sinkAPI;

import cn.chatdoge.fink113.source.ClickSource2;
import cn.chatdoge.fink113.utils.EventOnlyIdAndUrl;
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
 * @Description
 * @Author simon.mau
 * @Date 2023/12/3 9:43
 */
public class SinkToMysqlWithUpsert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1. 创建自定义数据源
        DataStreamSource<EventOnlyIdAndUrl> eventDS = env.addSource(new ClickSource2());

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://txy:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("mxw19910712@MYSQL")
                .build();

        SinkFunction<EventOnlyIdAndUrl> sink = JdbcSink.sink(
                "INSERT INTO test.mao2 (id, url) VALUES (?, ?) "
                        + "ON DUPLICATE KEY UPDATE url = VALUES(url)"
                ,
                new JdbcStatementBuilder<EventOnlyIdAndUrl>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventOnlyIdAndUrl event) throws SQLException {
                        preparedStatement.setString(1, event.getId());
                        preparedStatement.setString(2, event.getUrl());
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                jdbcConnectionOptions
        );

        eventDS.addSink(sink);

        env.execute();


    }

}
