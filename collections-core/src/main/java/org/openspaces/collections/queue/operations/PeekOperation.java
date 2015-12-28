package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueMetadata.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueMetadata.REMOVED_INDEXES_PATH;
import static org.openspaces.collections.queue.data.QueueMetadata.TAIL_PATH;

import java.util.HashSet;
import java.util.Set;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

public class PeekOperation extends SpaceEntriesAggregator<QueueHeadResult> {

    private static final long serialVersionUID = 1L;

    private transient QueueHeadResult result;
    
    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
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
