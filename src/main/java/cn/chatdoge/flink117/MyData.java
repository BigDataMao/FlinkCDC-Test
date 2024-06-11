package cn.chatdoge.flink117;

public class MyData {
    private int id;
    private String name;
    private String description;

    public MyData(int id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
