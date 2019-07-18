package com.test.table;

import com.google.gson.Gson;
import com.test.bean.Order;
import com.test.bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


import java.util.Properties;

/**
 * @author zhangzhiqiang
 * @date 2019/7/5 15:53
 */
public class TableJoinRowTimeStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        SingleOutputStreamOperator<User> user = env.addSource(new FlinkKafkaConsumer010<String>("user", new SimpleStringSchema(), p))
                .map(new MapFunction<String, User>() {
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

        SingleOutputStreamOperator<Order> order = env.addSource(new FlinkKafkaConsumer010<String>("order", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return new Gson().fromJson(value, Order.class);
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        return element.orderTime;
                    }
                });

        user.print("user:");
        order.print("order:");

        tableEnv.registerDataStream("users", user, "userId,name,age,sex,createTime,updateTime,rowTime.rowtime");
        tableEnv.registerDataStream("orders", order, "userId,orderId,price,orderTime,rowTime.rowtime");

        Table table = tableEnv.sqlQuery("select u.userId,u.name,o.orderId,o.price from users u inner join orders o on u.userId = o.userId where o.rowTime between u.rowTime - interval '1' minute and u.rowTime + interval '1' minute");
        table.printSchema();

        tableEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple4<String, String, String, String>>() {
        })).print("result:");

        env.execute("TableJoinRowTimeStream");
    }
}
