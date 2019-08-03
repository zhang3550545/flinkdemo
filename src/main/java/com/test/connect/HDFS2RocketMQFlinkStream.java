package com.test.connect;


import com.google.gson.JsonObject;
import flink.RocketMQConfig;
import flink.RocketMQSink;
import flink.common.selector.DefaultTopicSelector;
import flink.common.serialization.SimpleKeyValueSerializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static flink.common.serialization.SimpleKeyValueSerializationSchema.DEFAULT_KEY_FIELD;
import static flink.common.serialization.SimpleKeyValueSerializationSchema.DEFAULT_VALUE_FIELD;

public class HDFS2RocketMQFlinkStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);
        tableEnv.connect(new FileSystem().path("D:/workspacejava/flinkdemo/data/2019-08-03"))
                .withSchema(
                        new Schema()
                                .field("name", Types.STRING)
                                .field("age", Types.STRING)
                                .field("sex", Types.STRING)
                                .field("id", Types.STRING)
                )
                .withFormat(
                        new OldCsv()
                                .field("name", Types.STRING)
                                .field("age", Types.STRING)
                                .field("sex", Types.STRING)
                                .field("id", Types.STRING)
                                .fieldDelimiter(",")
                )
                .inAppendMode()
                .registerTableSource("original");

        Table table = tableEnv.sqlQuery("select * from original");
        SingleOutputStreamOperator<Map<String, String>> ds = tableEnv.toAppendStream(table, Row.class)
                .map(new MapFunction<Row, Map<String, String>>() {
                    @Override
                    public Map<String, String> map(Row row) throws Exception {
                        String name = (String) row.getField(0);
                        String age = (String) row.getField(1);
                        String sex = (String) row.getField(2);
                        String id = (String) row.getField(3);
                        JsonObject obj = new JsonObject();
                        obj.addProperty("name", name);
                        obj.addProperty("age", age);
                        obj.addProperty("sex", sex);
                        obj.addProperty("id", id);
                        String value = obj.toString();
                        System.out.println(value);
                        String key = "uuid_" + System.currentTimeMillis();
                        HashMap<String, String> map = new HashMap<>();
                        map.put(DEFAULT_KEY_FIELD, key);
                        map.put(DEFAULT_VALUE_FIELD, value);
                        return map;
                    }
                });

        ds.print();

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        int msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05;
        producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel));

        ds
                .addSink(
                        new RocketMQSink<>(
                                new SimpleKeyValueSerializationSchema()
                                , new DefaultTopicSelector("flink-hdfs-rmq")
                                , producerProps
                        ).withBatchFlushOnCheckpoint(true)
                ).name("rocketmq-sink");

        sEnv.execute("HDFS2RocketMQFlinkStream");
    }
}
