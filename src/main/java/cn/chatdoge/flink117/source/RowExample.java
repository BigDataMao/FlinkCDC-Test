package cn.chatdoge.flink117.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @Description: Row类型的使用
 * @Author: Simon Mau
 * @Date: 2023/12/11 11:25
 */
public class RowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义字段类型信息
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class)
        };

        // 定义字段名
        String[] fieldNames = new String[]{
                "name",
                "age"
        };

        // 创建 RowTypeInfo 对象
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        // 创建含有 Row 类型元素的数据流
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 25),
                Row.of("Bob", 30)
        ).returns(rowTypeInfo);

        // 使用 Row 类型的数据流进行处理或转换操作
        // 示例：打印姓名和年龄
        dataStream.map(row -> {
            String name = (String) row.getField(0); // 通过索引获取字段的值
            Integer age = (Integer) row.getField("age"); // 通过名称获取字段的值
            return name + " - " + age;
        }).print();

        env.execute("Row Example");
    }
}
