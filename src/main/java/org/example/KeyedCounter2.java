package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedCounter2 extends KeyedProcessFunction<Integer, Integer, Tuple2<Integer, Integer>>
    implements CheckpointedFunction {

  private ValueState<Integer> keyCounterState;
  private int keyCounter = -1;

  public void initializeState(FunctionInitializationContext context) throws Exception {
    keyCounterState = context.getKeyedStateStore()
        .getState(new ValueStateDescriptor<>("keyCounter", Integer.class));
  }

  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    keyCounterState.update(keyCounter);
  }

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
  }
}