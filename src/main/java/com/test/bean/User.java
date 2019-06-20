package com.test.bean;

public class User {
    public String userId;
    public String name;
    public String age;
    public String sex;
    public long createTime;
    public long updateTime;

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", sex='" + sex + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
