package org.openspaces.collections.queue.operations;

import java.util.Set;

public class QueueHeadTransformer {

    public QueueHeadResult transform(long head, long tail, Set<Long> removedIndexes) {
        if (head == tail) {
            throw new IllegalArgumentException("Queue should not be empty");
        }

        // skip elements that were removed with iterator.remove()
        long nextNonRemovedIndex = nextNonRemovedIndex(head, removedIndexes);
        return nextNonRemovedIndex > tail ? QueueHeadResult.emptyQueueResult(tail)
                : QueueHeadResult.headIndexResult(nextNonRemovedIndex);
    }
    

    /**
     * returns next non removed index, as a side effect cleans up 'removedIndexes' set removing indexes we no longer need
     */
    private long nextNonRemovedIndex(final long index, Set<Long> removedIndexes) {
        long next = index + 1;
        while (removedIndexes.remove(next)) {
            next++;
        }
        return next;
    }
}
