package com.test.cep;


import com.google.gson.Gson;


import com.test.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class CEPDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        p.setProperty("group.id", "test");
        DataStreamSource<String> ds = sEnv.addSource(new FlinkKafkaConsumer010<String>("cep", new SimpleStringSchema(), p));

        KeyedStream<Event, String> keyedStream = ds
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return new Gson().fromJson(value, Event.class);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
                    @Override
                    public long extractAscendingTimestamp(Event element) {

                        return element.getTimestamp();
                    }
                }).keyBy(new KeySelector<Event, String>() {

                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.getDriverId();
                    }
                });


        Pattern<Event, Event> pattern = Pattern.<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getEvent().equals("login");
                    }
                })
                .next("second").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getEvent().equals("my");
                    }
                })
                .followedBy("end").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getEvent().equals("ling quan");
                    }
                })
                .within(Time.minutes(5))
                .timesOrMore(5);


        PatternStream<Event> patternStream = CEP.pattern(keyedStream, pattern);
        patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
                out.collect(match.toString());
            }
        }).print();

        sEnv.execute("CEP2");
    }
}
