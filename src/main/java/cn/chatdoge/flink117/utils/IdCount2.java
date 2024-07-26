package cn.chatdoge.flink117.utils;

/**
 * @Description
 * @Author simon.mau
 * @Date 2023/12/5 20:43
 */
public class IdCount2 {
    private String id;
    private Integer idCount;

    public IdCount2() {
    };

    public IdCount2(String id, Integer idCount) {
        this.id = id;
        this.idCount = idCount;
    };

    @Override
    public String toString() {
        return IdCount2.class.getSimpleName() +
                "{" +
                "id='" + id + '\'' +
                ", idCount=" + idCount +
                '}';
    };

    // 重写equals方法
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IdCount2) { // 判断是否是WaterSensor类型
            IdCount2 idCount = (IdCount2) obj;
            return this.id.equals(idCount.id) && this.idCount.equals(idCount.idCount);
        } else {
            return false;
        }
    };

    // 重写hashCode方法
    @Override
    public int hashCode() {
        return this.id.hashCode() + this.idCount.hashCode();
    };

    // 提供get/set方法
    public String getId() {
        return id;
    };

    public Integer getIdCount() {
        return idCount;
    };

    public void setId(String id) {
        this.id = id;
    };

    public void setIdCount(Integer idCount) {
        this.idCount = idCount;
    };




}
