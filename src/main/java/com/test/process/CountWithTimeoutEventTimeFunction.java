package com.test.process;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * registerEventTimeTimer 是基于Event Time注册定时器，触发时需要下一条数据的水印作对比
 * 需要查看相差的时间是否大于等于interval，大于等于interval则触发，因为已经到时间了，小于interval，不然不会触发定时器，因为没到指定的时间
 * 如果下一条水印不来，就没办法对比时间间隔是否大于interval，也就不会触发
 * <p>
 * 与registerProcessingTimeTimer不同，这个是与机器时间做比对，所以一到interval，就会进行触发操作
 */
public class CountWithTimeoutEventTimeFunction
        extends KeyedProcessFunction<Integer, UserAction, Tuple2<Integer, Long>> {

    private ValueState<CountWithTimestamp> state;
    private final long interval = 60000L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            UserAction value,
            Context ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {

        CountWithTimestamp current = state.value();

        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.getUserId();
        }

        current.count++;
        current.lastModified = ctx.timestamp();

        state.update(current);

        ctx.timerService().registerEventTimeTimer(current.lastModified + interval);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {

        CountWithTimestamp result = state.value();

        if (timestamp == result.lastModified + interval) {
            out.collect(new Tuple2<Integer, Long>(result.key, result.count));
            state.update(null);
            ctx.timerService().deleteEventTimeTimer(timestamp);
        }
    }
}