package cn.chatdoge.flink117;

import cn.chatdoge.flink117.utils.MyOracleDebeziumDeserializationSchema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;



public class TestOracleMy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("localhost")
                .port(31521)
                .database("XE") // Oracle SID
                .username("flinkuser")
                .password("flinkpw")
                .schemaList("flinkuser")
                .tableList("flinkuser.products")
                .deserializer(new MyOracleDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        String url = "jdbc:oracle:thin:@localhost:31521:XE";
        String driveName = "oracle.jdbc.OracleDriver";
        String userName = "flinkuser";
        String password = "flinkpw";
        String tableName = "flinkuser.products_cdc";

        SinkFunction<MyData> sink = JdbcSink.sink(
                "INSERT INTO " + tableName + " (id, name, description) VALUES (?, ?, ?)",
                (JdbcStatementBuilder<MyData>) (ps, t) -> {
                    ps.setInt(1, t.getId());
                    ps.setString(2, t.getName());
                    ps.setString(3, t.getDescription());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driveName)
                        .withUsername(userName)
                        .withPassword(password)
                        .build()
        );

        dataStreamSource.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject after = jsonObject.getJSONObject("after");
            int id = after.getIntValue("ID");
            String name = after.getString("NAME");
            String description = after.getString("DESCRIPTION");
            System.out.println("ID: " + id + ", NAME: " + name + ", DESCRIPTION: " + description);
            return new MyData(id, name, description);
        }).addSink(sink);

        env.execute("test flinkCDC-oracle");

    }
}

