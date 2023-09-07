package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyCounter extends KeyedProcessFunction<Integer, Integer, Tuple2<Integer, Integer>>
    implements CheckpointedFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCounter.class);

  private ValueState<Integer> keyCounterState;

  private int keyCounter;

  @Override
  public void processElement(final Integer value,
                             final Context context,
                             final Collector<Tuple2<Integer, Integer>> collector) throws Exception {


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

  @Override
  public void snapshotState(final FunctionSnapshotContext context) throws Exception {
    keyCounterState.update(keyCounter);
  }

  @Override
  public void initializeState(final FunctionInitializationContext context) throws Exception {
    keyCounterState = getRuntimeContext()
        .getState(new ValueStateDescriptor<>("keyCounter", Integer.class));
  }
}
