package com.test.bean;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Student {
    public String name;
    public int age;
    public String sex;
    public String sid;
    public long timestamp;

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", sid='" + sid + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
