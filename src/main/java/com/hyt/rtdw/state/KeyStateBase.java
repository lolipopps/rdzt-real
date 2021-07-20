package com.hyt.rtdw.state;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KeyStateBase {
    public static KeyedStream<Tuple2<Integer, Integer>, Tuple> before(StreamExecutionEnvironment env) {

        env.setParallelism(2);

        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new StateSource());

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = inputStream.keyBy(0);

        return keyedStream;
    }

    public static class StateSource implements SourceFunction<Tuple2<Integer, Integer>> {

        public Logger LOG = LoggerFactory.getLogger(StateSource.class);

        private static final long serialVersionUID = 1L;

        private int counter = 1;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (true) {
                ctx.collect(new Tuple2<>(counter % 5, counter));
                LOG.info("send data :{} ,{} ", counter % 5, counter);
                System.out.println("send data :" + counter % 5 + "," + counter);
                counter++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
        }
    }
}
