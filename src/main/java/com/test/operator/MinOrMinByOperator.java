package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class MinOrMinByOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "10.105.18.175:9092");

        DataStreamSource<String> source = sEnv.addSource(new FlinkKafkaConsumer010<String>("student", new SimpleStringSchema(), p));

        source.print();

        WindowedStream<Student, String, TimeWindow> windowedStream = source
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        return new Gson().fromJson(value, Student.class);
                    }
                })
                .keyBy(new KeySelector<Student, String>() {
                    @Override
                    public String getKey(Student value) throws Exception {
                        return value.sid;
                    }
                })
                .timeWindow(Time.minutes(1));

        SingleOutputStreamOperator<Student> min = windowedStream.min("age");
        min.print("min  :");

        SingleOutputStreamOperator<Student> minBy = windowedStream.minBy("age");
        minBy.print("minBy:");

        SingleOutputStreamOperator<Student> max = windowedStream.max("age");
        max.print("max  :");

        SingleOutputStreamOperator<Student> maxBy = windowedStream.maxBy("age");
        maxBy.print("maxBy:");

        sEnv.execute("MinOrMinByOperator");
    }

}