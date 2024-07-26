package cn.chatdoge.flink117.POJO;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Test {
    public static boolean isPojoClass(Class<?> pojoClass) {
        try {
            TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(pojoClass);
            return typeInfo instanceof PojoTypeInfo;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
        // 替换为你要测试的类
        boolean isPojo = isPojoClass(IdCount.class);

        if (isPojo) {
            System.out.println("This is a POJO class.");
        } else {
            System.out.println("This is not a POJO class.");
        }
    }
}