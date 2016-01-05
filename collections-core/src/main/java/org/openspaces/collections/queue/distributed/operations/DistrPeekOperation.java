package org.openspaces.collections.queue.distributed.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.HashSet;
import java.util.Set;

import static org.openspaces.collections.queue.distributed.data.DistrQueueMetadata.*;

public class DistrPeekOperation extends SpaceEntriesAggregator<DistrQueueHeadResult> {

    private static final long serialVersionUID = 1L;

    private transient DistrQueueHeadResult result;

    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void aggregate(SpaceEntriesAggregatorContext context) {
        final long tail = (long) context.getPathValue(TAIL_PATH);
        final long head = (long) context.getPathValue(HEAD_PATH);

        if (tail == head) {
            this.result = DistrQueueHeadResult.emptyQueueResult();
        } else {
            final Set<Long> removedIndexes = new HashSet<>((Set<Long>) context.getPathValue(REMOVED_INDEXES_PATH));
            this.result = new DistrQueueHeadTransformer().forwardQueueHead(head, tail, removedIndexes);
        }
    }

    @Override
    public DistrQueueHeadResult getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(DistrQueueHeadResult partitionResult) {
        this.result = partitionResult;
    }
}
