package org.openspaces.collections.queue.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.HashSet;
import java.util.Set;

import static org.openspaces.collections.queue.data.QueueMetadata.*;

public class PeekOperation extends SpaceEntriesAggregator<QueueHeadResult> {

    private static final long serialVersionUID = 1L;

    private transient QueueHeadResult result;
    
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
            this.result = QueueHeadResult.emptyQueueResult();
        } else {
            final Set<Long> removedIndexes = new HashSet<>((Set<Long>) context.getPathValue(REMOVED_INDEXES_PATH));
            this.result = new QueueHeadTransformer().forwardQueueHead(head, tail, removedIndexes);
        }
    }

    @Override
    public QueueHeadResult getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(QueueHeadResult partitionResult) {
        this.result = partitionResult;
    }
}
