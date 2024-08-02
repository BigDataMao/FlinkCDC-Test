package cn.chatdoge.flink117.deserializationSchema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CustomStringDeserializationSchema implements DeserializationSchema<String> {


    @Override
    public String deserialize(byte[] bytes) throws IOException {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
