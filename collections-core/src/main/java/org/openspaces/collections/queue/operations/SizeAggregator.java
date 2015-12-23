package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;

import java.io.Serializable;

import org.openspaces.collections.queue.operations.SizeAggregator.Result;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

/**
 * @author Oleksiy_Dyagilev
 */
public class SizeAggregator extends SpaceEntriesAggregator<Result> {

    private static final long serialVersionUID = 1L;

    private final String ALIAS = "size";

    private transient Result sizeResult;
    
    @Override
    public String getDefaultAlias() {
        return ALIAS;
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Long tail = (Long) context.getPathValue(TAIL_PATH);
        Long head = (Long) context.getPathValue(HEAD_PATH);

        final long size = tail - head;
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
    public static class Result implements Serializable {
        private int size;

        public Result() {
        }

        public Result(int size) {
            this.size = size;
        }

        public int getSize() {
            return size;
        }
    }
}

