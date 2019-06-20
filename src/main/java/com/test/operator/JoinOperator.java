package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.Order;
import com.test.bean.User;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import scala.Tuple7;

import java.util.Properties;

/**
 * 测试数据
 * <p>
 * User{userId='1001', name='caocao', age='20', sex='null', createTime=1561023040338, updateTime=1561023040338}
 * Order{orderId='1001', userId='1001', price='10', timestamp=1561023042640}
 * Order{orderId='1002', userId='1001', price='20', timestamp=1561023043649}
 * Order{orderId='1003', userId='1002', price='30', timestamp=1561023044651}
 * order join user> (1001,caocao,20,1001,1001,10,1561023042640)
 * user join order> (1001,caocao,20,1001,1001,10,1561023042640)
 * order join user> (1001,caocao,20,1001,1002,20,1561023043649)
 * user join order> (1001,caocao,20,1001,1002,20,1561023043649)
 */

public class JoinOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "10.105.18.175:9092");
        DataStreamSource<String> order = sEnv.addSource(new FlinkKafkaConsumer010<String>("order", new SimpleStringSchema(), p));
        DataStreamSource<String> user = sEnv.addSource(new FlinkKafkaConsumer010<String>("user", new SimpleStringSchema(), p));

        SingleOutputStreamOperator<Order> operator = order.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                return new Gson().fromJson(value, Order.class);
            }
        });

        operator.print();

        SingleOutputStreamOperator<User> operator1 = user.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                return new Gson().fromJson(value, User.class);
            }
        });

        operator1.print();

        // 使用的是inner join 具体看测试数据
        operator.join(operator1)
                .where(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.userId;
                    }
                })
                .equalTo(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User value) throws Exception {
                        return value.userId;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .trigger(ProcessingTimeTrigger.create())
                .apply(new JoinFunction<Order, User, Tuple7<String, String, String, String, String, String, Long>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, String, Long> join(Order order, User user) throws Exception {
                        return new Tuple7<>(user.userId, user.name, user.age, order.userId, order.orderId, order.price, order.timestamp);
                    }
                })
                .print("order join user");


        operator1.join(operator)
                .where(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User value) throws Exception {
                        return value.userId;
                    }
                })
                .equalTo(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.userId;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .trigger(ProcessingTimeTrigger.create())
                .apply(new JoinFunction<User, Order, Tuple7<String, String, String, String, String, String, Long>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, String, Long> join(User user, Order order) throws Exception {
                        return new Tuple7<>(user.userId, user.name, user.age, order.userId, order.orderId, order.price, order.timestamp);
                    }
                })
                .print("user join order");


        sEnv.execute("JoinOperator");
    }
}
