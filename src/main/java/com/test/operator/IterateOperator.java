package com.test.operator;

import com.google.gson.Gson;

import com.test.bean.People;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class IterateOperator {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");

        DataStreamSource<String> source = sEnv.addSource(new FlinkKafkaConsumer010<String>("people", new SimpleStringSchema(), p));

        IterativeStream<People> iterate = source.map(new MapFunction<String, People>() {
            @Override
            public People map(String value) throws Exception {
                return new Gson().fromJson(value, People.class);
            }
        }).iterate();


        SingleOutputStreamOperator<People> feedback = iterate.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People value) throws Exception {
                return value.name().equals("caocao");
            }
        });

        feedback.print("feedback:");// 如果有符合feedback过滤条件的数据，比如：name为caocao的，会持续不断的循环输出

        iterate.closeWith(feedback);

        SingleOutputStreamOperator<People> result = iterate.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People value) throws Exception {
                return !value.name().equals("caocao");
            }
        });

        result.print("result:");

        sEnv.execute("IterateOperator");
    }
}
