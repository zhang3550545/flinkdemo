package com.test.bean;

public class Teacher {

    public String name;
    public int age;
    public String sex;
    public String tid;
    public long timestamp;
    public String classId;

    @Override
    public String toString() {
        return "Teacher{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", tid='" + tid + '\'' +
                ", timestamp=" + timestamp +
                ", classId='" + classId + '\'' +
                '}';
    }
}
