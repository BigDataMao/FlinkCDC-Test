package cn.chatdoge.flink117.utils;


import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;


public class MyOracleDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 获取库名,表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String table = split[2];

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        // 获取before
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJSON = new JSONObject();
        if (before != null) {
            before.schema().fields().forEach(f -> {
                beforeJSON.put(f.name(), before.get(f));
            });
        }
        ;

        // 获取after
        Struct after = value.getStruct("after");
        JSONObject afterJSON = new JSONObject();
        if (after != null) {
            after.schema().fields().forEach(f -> {
                afterJSON.put(f.name(), after.get(f));
            });
        }
        ;

        // 封装结果数据
        JSONObject result = new JSONObject();
        result.put("database", database);
        result.put("table", table);
        result.put("type", operation.toString().toLowerCase());
        result.put("before", beforeJSON);
        result.put("after", afterJSON);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}



