package cn.chatdoge.fink113;

// Builder模式示例
public class Car {
    private final String owner;
    private final Integer price;

    protected Car(String owner, Integer price) {
        this.owner = owner;
        this.price = price;
    }

    public String getOwner() {
        return owner;
    }

    public Integer getPrice() {
        return price;
    }

    public static class CarBuilder{
        private String owner;
        private Integer price;

        public CarBuilder setOwner(String owner) {
            this.owner = owner;
            return this;
        }

        public CarBuilder setPrice(Integer price) {
            this.price = price;
            return this;
        }

        public Car build(){
            return new Car(owner, price);
        }
    }

    // 写个开车的方法
    public void run(){
        System.out.println(this.owner + " is driving the car.");
    }

}
