package cn.chatdoge.flink117.utils;

// 类的注释

import java.sql.Timestamp;

public class Event {
    private String id;
    private Long ts;
    private String url;

    public Event() {
    }

    public Event(String id, Long ts, String url) {
        this.id = id;
        this.ts = ts;
        this.url = url;
    }

    @Override
    public String toString() {
        return Event.class.getSimpleName() +
                "{" +
                "id='" + id + '\'' +
                ", ts=" + new Timestamp(ts) +  // 将时间戳转换成时间格式
                ", vc=" + url +
                '}';
    }

    // 重写equals方法
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) { // 判断是否是WaterSensor类型
            Event event = (Event) obj;
            return this.id.equals(event.id) && this.ts.equals(event.ts) && this.url.equals(event.url);
        } else {
            return false;
        }
    }

    // 重写hashCode方法
    @Override
    public int hashCode() {
        return this.id.hashCode() + this.ts.hashCode() + this.url.hashCode();
    }

    // 提供get/set方法
    public String getId() {
        return id;
    }

    public Long getTs() {
        return ts;
    }

    public String getUrl() {
        return url;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setUrl(String url) {
        this.url = url;
    }

};