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

  }

  @Override
  public void processElement(final Integer value,
                             final Context context,
                             final Collector<Tuple2<Integer, Integer>> collector) throws Exception {


    int keyCounter;
    Integer valueFromState = keyCounterState.value();
    if (valueFromState == null) {
      keyCounterState.update(0);
      keyCounter = 0;
    } else {
      keyCounter = valueFromState;
    }

    collector.collect(Tuple2.of(context.getCurrentKey(), ++keyCounter));
    keyCounterState.update(keyCounter);
  }
}
