package com.test.process;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class CountWithTimeoutFunction
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

        // retrieve the current count
        CountWithTimestamp current = state.value();

        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.getUserId();
        }

        current.count++;
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
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