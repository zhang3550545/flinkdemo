package com.test.process;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class CountWithTimeoutProcessingTimeFunction
        extends KeyedProcessFunction<Integer, UserAction, Tuple2<Integer, Long>> {

    private ValueState<CountWithTimestamp> state;

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

        ctx.timerService().registerProcessingTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {

        CountWithTimestamp result = state.value();

        if (timestamp == result.lastModified + 60000) {
            out.collect(new Tuple2<Integer, Long>(result.key, result.count));
            state.update(null);
            ctx.timerService().deleteEventTimeTimer(timestamp);
        }
    }
}