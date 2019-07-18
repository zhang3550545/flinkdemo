package com.test.table;

import com.google.gson.Gson;
import com.test.bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author zhangzhiqiang
 * @date 2019/7/5 14:10
 */
public class TableWindowRowStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        SingleOutputStreamOperator<User> ds = env
                .addSource(new FlinkKafkaConsumer010<String>("user", new SimpleStringSchema(), p)).map(new MapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        return new Gson().fromJson(value, User.class);
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<User>() {
                    @Override
                    public long extractAscendingTimestamp(User element) {
                        return element.createTime;
                    }
                });

        // 不能用 user
        tableEnv.registerDataStream("users", ds, "userId,name,age,sex,createTime,updateTime,rowTime.rowtime");

        Table table = tableEnv.sqlQuery("SELECT userId,name,age,sex,createTime,updateTime,rowTime FROM users WHERE rowTime BETWEEN rowTime - INTERVAL '1' MINUTE AND rowTime");
        table.printSchema();
        tableEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple7<String, String, String, String, Long, Long, Timestamp>>() {
        })).print("result:");

        // 输出的row时间是UTC时区的时间
        env.execute("TableWindowProcStream");
    }
}
