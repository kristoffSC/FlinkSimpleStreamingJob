/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		env.enableCheckpointing(5_000, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(1);

		// pipeline
		env
			.addSource(new CheckpointCountingSource(100, 60))
			.keyBy(value -> value % 10)
			.process(new KeyCounter())
			.addSink(new ConsoleSink());


		// Execute program, beginning computation.
		env.execute("FLink Toy Streaming job.");
	}
}
