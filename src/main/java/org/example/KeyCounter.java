package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyCounter extends KeyedProcessFunction<Integer, Integer, Tuple2<Integer, Integer>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCounter.class);

  private ValueState<Integer> keyCounterState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    LOGGER.info("Key counter state init.");
    keyCounterState = getRuntimeContext()
        .getState(new ValueStateDescriptor<>("keyCounter", Integer.class));

    if (keyCounterState.value() == null) {
      keyCounterState.update(0);
    }
  }

  @Override
  public void processElement(Integer value,
                             Context context,
                             Collector<Tuple2<Integer, Integer>> collector) throws Exception {


    int keyCounter = keyCounterState.value();
    collector.collect(Tuple2.of(context.getCurrentKey(), ++keyCounter));
    keyCounterState.update(keyCounter);
  }
}
