package com.test.operator;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

public class WaterMarkSourceStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        p.setProperty("group.id", "test");
        String type = "{\"type\":\"object\",\"properties\":{\"Msg\":{\"type\":\"object\",\"properties\":{\"MemberId\":{\"type\":\"string\"},\"LogActionName__Special__\":{\"type\":\"string\"},\"LogControllerName__Special__\":{\"type\":\"string\"},\"LogTime__Special__\":{\"type\":\"string\"}}}}}";

        FlinkKafkaConsumer010<Row> consumer010 = new FlinkKafkaConsumer010<>("app_scene_roominfo", new JsonRowDeserializationSchema(type), p);

        consumer010.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                // 取的是Msg
                Row field = (Row) element.getField(0);
                int arity = field.getArity();
                String time = (String) field.getField(arity - 1);

                Date date = null;
                try {
                    date = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss").parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return date == null ? 0 : date.getTime();
            }
        });
        DataStreamSource<Row> source = env.addSource(consumer010);

        source.print();

        env.execute("WaterMarkSourceStream");
    }
}
