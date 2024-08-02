package cn.chatdoge.flink117.deserializationSchema;

import cn.chatdoge.flink117.POJO.Order;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class CustomOrderDeserializationSchema implements DeserializationSchema<Order> {

    @Override
    public Order deserialize(byte[] message) throws IOException {
        JSONObject jsonObject = JSON.parseObject(message);

        Order order = new Order();
        order.setId(jsonObject.getInteger("id"));
        order.setName(jsonObject.getString("name"));

        // 解析 event_time 字段，处理空格并转换为 OffsetDateTime
        String eventTimeString = jsonObject.getString("event_time");
        // 替换空格为 'T'，并将其转换为 OffsetDateTime
        OffsetDateTime eventTime = OffsetDateTime.parse(eventTimeString.replace(" ", "T"));
        order.setEvent_time(eventTime);

        return order;
    }

    @Override
    public boolean isEndOfStream(Order order) {
        return false;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}
