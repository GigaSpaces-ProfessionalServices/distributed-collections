package org.openspaces.collections.queue.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;
import org.openspaces.collections.queue.operations.SizeOperation.Result;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.openspaces.collections.queue.data.QueueMetadata.*;

/**
 * @author Oleksiy_Dyagilev
 */
public class SizeOperation extends SpaceEntriesAggregator<Result> {

    private static final long serialVersionUID = 1L;

    private transient Result sizeResult;

    @Override
    public String getDefaultAlias() {
        return "size";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Long tail = (Long) context.getPathValue(TAIL_PATH);
        Long head = (Long) context.getPathValue(HEAD_PATH);

        long size = tail - head - (Integer) context.getPathValue(REMOVED_INDEXES_SIZE_PATH);

        this.sizeResult = new Result((int) size);
    }

    @Override
    public Result getIntermediateResult() {
        return sizeResult;
    }

    @Override
    public void aggregateIntermediateResult(Result partitionResult) {
        this.sizeResult = partitionResult;
    }

    /**
     * Operation result
     */
    public static class Result implements Externalizable {
        private int size;

        public Result() {
        }

        public Result(int size) {
            this.size = size;
        }

        public int getSize() {
            return size;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(getSize());
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.size = in.readInt();
        }
    }
}

