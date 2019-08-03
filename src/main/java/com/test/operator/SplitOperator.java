package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.People;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SplitOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");

        DataStreamSource<String> source = sEnv.addSource(new FlinkKafkaConsumer010<String>("people", new SimpleStringSchema(), p));

        SplitStream<People> splitStream = source.map(new MapFunction<String, People>() {
            @Override
            public People map(String value) throws Exception {
                return new Gson().fromJson(value, People.class);
            }
        }).split(new OutputSelector<People>() { // split可以将一个流，通过打Tag的方式，split成多个流
            @Override
            public Iterable<String> select(People value) {
                List<String> list = new ArrayList<>();
                if (value.getSex().equals("male")) {
                    list.add("male");
                } else {
                    list.add("female");
                }
                return list;
            }
        });


        // SplitStream流 通过select("tag")获取DataStream流
        DataStream<People> male = splitStream.select("male");
        male.print("male:");

        DataStream<People> female = splitStream.select("female");
        female.print("female:");

        // 将流合并
        DataStream<People> union = male.union(female);
        union.print("union:");

        sEnv.execute("SplitOperator");
    }
}
