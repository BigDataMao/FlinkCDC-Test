package cn.chatdoge.finkCDC;

import cn.chatdoge.finkCDC.utils.MyOracleDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        dataStreamSource.print();

        env.execute("test flinkCDC-oracle");

    }
}
