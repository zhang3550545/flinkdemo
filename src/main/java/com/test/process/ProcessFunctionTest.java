package com.test.process;

import com.google.gson.Gson;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.text.ParseException;
import java.util.Properties;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer010<String>("user_view_log", new SimpleStringSchema(), p));
        ds.print();

        ds
                .map(new MapFunction<String, UserAction>() {
                    @Override
                    public UserAction map(String value) throws Exception {
                        return new Gson().fromJson(value, UserAction.class);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserAction>() {
                    @Override
                    public long extractAscendingTimestamp(UserAction element) {
                        try {
                            return element.getUserActionTime();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0;
                    }
                })
                .keyBy(new KeySelector<UserAction, Integer>() {
                    @Override
                    public Integer getKey(UserAction value) throws Exception {
                        return value.getUserId();
                    }
                })
                // CountWithTimeoutEventTimeFunction：注册的是EventTime注册器
                // CountWithTimeoutProcessingTimeFunction：注册的是ProcessingTime注册器
                .process(new CountWithTimeoutEventTimeFunction())
                .print();

        env.execute("ProcessFunctionTest");
    }
}
