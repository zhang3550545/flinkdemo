package com.test.bean;

public class Order {
    public String orderId;
    public String userId;
    public String price;
    public long orderTime;

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", price='" + price + '\'' +
                ", orderTime=" + orderTime +
                '}';
    }
}
