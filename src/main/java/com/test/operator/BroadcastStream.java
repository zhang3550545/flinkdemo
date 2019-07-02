package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.Order;
import com.test.bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author zhangzhiqiang
 * @date 2019/7/2 16:29
 */
public class BroadcastStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "dev-hdp-2.huazhu.com:6667,dev-hdp-3.huazhu.com:6667,dev-hdp-4.huazhu.com:6667");

        SingleOutputStreamOperator<User> user = env
                .addSource(new FlinkKafkaConsumer010<String>("user", new SimpleStringSchema(), p))
                .map(new MapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        return new Gson().fromJson(value, User.class);
                    }
                });

        user.print("user: ");

        SingleOutputStreamOperator<Order> order = env
                .addSource(new FlinkKafkaConsumer010<String>("order", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return new Gson().fromJson(value, Order.class);
                    }
                });

        order.print("order: ");

        MapStateDescriptor<String, User> descriptor = new MapStateDescriptor<String, User>("user", String.class, User.class);
        org.apache.flink.streaming.api.datastream.BroadcastStream<User> broadcast = user.broadcast(descriptor);
        BroadcastConnectedStream<Order, User> connect = order.connect(broadcast);

        connect
                .process(new BroadcastProcessFunction<Order, User, String>() {
                    @Override
                    public void processElement(Order value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, User> broadcastState = ctx.getBroadcastState(descriptor);
                        // 从广播中获取对应的key的value
                        User user = broadcastState.get(value.userId);
                        if (user != null) {
                            Tuple8<String, String, String, Long, String, String, String, Long> result = new Tuple8<>(
                                    value.userId,
                                    value.orderId,
                                    value.price,
                                    value.timestamp,
                                    user.name,
                                    user.age,
                                    user.sex,
                                    user.createTime
                            );
                            String s = result.toString();
                            out.collect(s);
                        }
                    }

                    @Override
                    public void processBroadcastElement(User value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, User> broadcastState = ctx.getBroadcastState(descriptor);
                        broadcastState.put(value.userId, value);
                    }
                })
                .print("result: ");

        env.execute("broadcast: ");
    }
}

//user: > User{userId='1002', name='caocao', age=20, sex='male', createTime='1562059847905'}
//order: > Order{userId='1002', orderId='1001', price='10', orderTime='1562059869825'}
//result: > (1002,1001,10,1562059869825,caocao,20,male,1562059847905)