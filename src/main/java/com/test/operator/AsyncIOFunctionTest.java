package com.test.operator;

import com.google.gson.Gson;
import com.mysql.jdbc.Connection;
import com.test.bean.Order;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AsyncIOFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "dev-hdp-2.huazhu.com:6667,dev-hdp-3.huazhu.com:6667,dev-hdp-4.huazhu.com:6667");

        DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer010<String>("order", new SimpleStringSchema(), p));
        ds.print();

        SingleOutputStreamOperator<Order> order = ds
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return new Gson().fromJson(value, Order.class);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        try {
                            return element.timestamp;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0;
                    }
                })
                .keyBy(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.userId;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .maxBy("orderTime");

        SingleOutputStreamOperator<Tuple7<String, String, Integer, String, String, String, Long>> operator = AsyncDataStream
                .unorderedWait(order, new RichAsyncFunction<Order, Tuple7<String, String, Integer, String, String, String, Long>>() {

                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        Class.forName("com.mysql.jdbc.Driver");
                        connection = (Connection) DriverManager.getConnection("url", "user", "pwd");
                        connection.setAutoCommit(false);
                    }

                    @Override
                    public void asyncInvoke(Order input, ResultFuture<Tuple7<String, String, Integer, String, String, String, Long>> resultFuture) throws Exception {
                        List<Tuple7<String, String, Integer, String, String, String, Long>> list = new ArrayList<>();
                        // 在 asyncInvoke 方法中异步查询数据库
                        String userId = input.userId;
                        Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery("select name,age,sex from user where userid=" + userId);
                        if (resultSet != null && resultSet.next()) {
                            String name = resultSet.getString("name");
                            int age = resultSet.getInt("age");
                            String sex = resultSet.getString("sex");
                            Tuple7<String, String, Integer, String, String, String, Long> res = Tuple7.of(userId, name, age, sex, input.orderId, input.price, input.timestamp);
                            list.add(res);
                        }

                        // 将数据搜集
                        resultFuture.complete(list);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        if (connection != null) {
                            connection.close();
                        }
                    }
                }, 5000, TimeUnit.MILLISECONDS);

        operator.print();


        env.execute("AsyncIOFunctionTest");
    }
}