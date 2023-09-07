package org.example;

import java.util.StringJoiner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSink extends RichSinkFunction<Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    private transient int recordCount = 0;

    @Override
    public void invoke(Tuple2<Integer, Integer> row, Context context) {

        StringJoiner joiner = new StringJoiner(", ");
        joiner.add(String.valueOf(row.f0)).add(String.valueOf(row.f1));

        LOG.info("Row content: " + joiner);
        recordCount++;
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("Total number of records (not checkpointed): " + recordCount);
    }
}
