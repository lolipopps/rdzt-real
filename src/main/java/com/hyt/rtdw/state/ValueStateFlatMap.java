package com.hyt.rtdw.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ValueStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    public static Logger LOG = LoggerFactory.getLogger(ValueStateFlatMap.class);

    public transient ValueState<Tuple2<Integer, Integer>> valueState;


    @Override
    public void open(Configuration config) {
        LOG.info("{},{}", Thread.currentThread().getName(), "恢复或初始化状态");
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<>(
                        "ValueStateFlatMap",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }));
        valueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        Tuple2<Integer, Integer> currentSum = valueState.value();
        String threadName = Thread.currentThread().getName();
        LOG.info("{} currentSum before: {},input :{}", threadName, currentSum, input);
        if (currentSum == null) {
            currentSum = input;
        } else {
            currentSum.f1 = currentSum.f1 + input.f1;
        }
        if (currentSum.f1 % 10 >= 6) {
            out.collect(currentSum);
            valueState.clear();
        } else {
            valueState.update(currentSum);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = KeyStateBase.before(env);

        DataStream<Tuple2<Integer, Integer>> resultStream = keyedStream.flatMap(new ValueStateFlatMap());

        resultStream.print("输出结果:");

        env.execute("Intsmaze ValueStateFlatMap");
    }
}
