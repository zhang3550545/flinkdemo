package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class PunctuatedWatermark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);


        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "10.105.18.175:9092");

        SingleOutputStreamOperator<Student> student = sEnv
                .addSource(new FlinkKafkaConsumer010<String>("student", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        return new Gson().fromJson(value, Student.class);
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Student>() {

                    @Override
                    public Watermark checkAndGetNextWatermark(Student lastElement, long extractedTimestamp) {
                        System.out.println("extractedTimestamp: " + extractedTimestamp);
                        return new Watermark(lastElement.timestamp);
                    }

                    @Override
                    public long extractTimestamp(Student element, long previousElementTimestamp) {
                        System.out.println("lastElement.timestamp: " + previousElementTimestamp);
                        return element.timestamp;
                    }
                });

        student.print();

        sEnv.execute("PunctuatedWatermark");
    }
}
