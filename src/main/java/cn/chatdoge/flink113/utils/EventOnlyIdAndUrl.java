package cn.chatdoge.flink113.utils;

/**
 * @Description
 * @Author simon.mau
 * @Date 2023/12/3 22:38
 */
public class EventOnlyIdAndUrl {
    private String id;
    private String url;

    public EventOnlyIdAndUrl() {
    }

    public EventOnlyIdAndUrl(String id, String url) {
        this.id = id;
        this.url = url;
    }

    @Override
    public String toString() {
        return EventOnlyIdAndUrl.class.getSimpleName() +
                "{" +
                "id='" + id + '\'' +
                ", url=" + url +
                '}';
    }

    // 重写equals方法
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EventOnlyIdAndUrl) {
            EventOnlyIdAndUrl event = (EventOnlyIdAndUrl) obj;
            return this.id.equals(event.id) && this.url.equals(event.url);
        } else {
            return false;
        }
    }

    // 重写hashCode方法
    @Override
    public int hashCode() {
        return this.id.hashCode() + this.url.hashCode();
    }

    // 提供get/set方法
    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }
}
