package org.example;

import java.util.Collections;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each of the source operators outputs records in given number of checkpoints. Number of records
 * per checkpoint is constant between checkpoints, and defined by user. When all records are
 * emitted, the source waits for two more checkpoints until it finishes.
 * <p>
 * All credits for this implementation goes to <b>Grzegorz Kolakowski<b> who implemented the
 * original version of this class for end2end tests. This class was copied from his Pull Request
 * here.
 */
public class CheckpointCountingSource extends RichParallelSourceFunction<Integer>
    implements CheckpointListener, CheckpointedFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointCountingSource.class);

    private final int numberOfCheckpoints;

    private final int recordsPerCheckpoint;

    private transient ListState<Integer> nextValueState;

    private int nextKey;

    private volatile boolean isCanceled;

    private volatile boolean waitingForCheckpoint;

    public CheckpointCountingSource(int recordsPerCheckpoint, int numberOfCheckpoints) {

        this.numberOfCheckpoints = numberOfCheckpoints;
        this.recordsPerCheckpoint = recordsPerCheckpoint;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("nextKey", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextKey = nextValueState.get().iterator().next();
        }
        waitingForCheckpoint = false;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        LOGGER.info("Run subtask={}; attempt={}.",
            getRuntimeContext().getIndexOfThisSubtask(),
            getRuntimeContext().getAttemptNumber());

        sendRecordsUntil(numberOfCheckpoints, ctx);
        idleUntilNextCheckpoint(ctx);
        LOGGER.info("Source task done; subtask={}.",
            getRuntimeContext().getIndexOfThisSubtask());
    }

    private void sendRecordsUntil(int targetCheckpoints, SourceContext<Integer> ctx)
        throws InterruptedException {
        while (!isCanceled && nextKey < targetCheckpoints * recordsPerCheckpoint) {
            synchronized (ctx.getCheckpointLock()) {
                emitRecordsBatch(recordsPerCheckpoint, ctx);
                waitingForCheckpoint = true;
            }
            LOGGER.info("Waiting for checkpoint to complete; subtask={}.",
                getRuntimeContext().getIndexOfThisSubtask());
            while (waitingForCheckpoint) {
                Thread.sleep(1);
            }
        }
        nextKey = 0;
    }

    private void emitRecordsBatch(int batchSize, SourceContext<Integer> ctx) {
        for (int i = 0; i < batchSize; i++) {
            ctx.collect(nextKey += 1);
        }

        LOGGER.info("Emitted {} records (total {}); subtask={}.", batchSize, nextKey,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    private void idleUntilNextCheckpoint(SourceContext<Integer> ctx) throws
        InterruptedException {
        // Idle until the next checkpoint completes to avoid any premature job termination and
        // race conditions.
        LOGGER.info("Waiting for an additional checkpoint to complete; subtask={}.",
            getRuntimeContext().getIndexOfThisSubtask());
        synchronized (ctx.getCheckpointLock()) {
            waitingForCheckpoint = true;
        }
        while (waitingForCheckpoint) {
            Thread.sleep(1L);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextKey));
        LOGGER.info("state snapshot done; checkpointId={}; subtask={}.",
            context.getCheckpointId(),
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        waitingForCheckpoint = false;
        LOGGER.info("Checkpoint {} complete; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        LOGGER.info("Checkpoint {} aborted; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
        CheckpointListener.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }

}
