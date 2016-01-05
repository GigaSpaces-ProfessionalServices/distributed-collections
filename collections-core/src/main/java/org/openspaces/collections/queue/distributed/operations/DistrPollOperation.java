package org.openspaces.collections.queue.distributed.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.HashSet;
import java.util.Set;

import static org.openspaces.collections.queue.distributed.data.DistrQueueMetadata.*;

/**
 * @author Oleksiy_Dyagilev
 */
public class DistrPollOperation extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    @Override
    @SuppressWarnings("unchecked")
    public Object change(MutableServerEntry entry) {
        final long tail = (long) entry.getPathValue(TAIL_PATH);
        final long head = (long) entry.getPathValue(HEAD_PATH);

        if (tail == head) {
            return DistrQueueHeadResult.emptyQueueResult();
        }

        // skip elements that were removed with iterator.remove()
        final Set<Long> removedIndexes = new HashSet<>((Set<Long>) entry.getPathValue(REMOVED_INDEXES_PATH));

        DistrQueueHeadResult result = new DistrQueueHeadTransformer().forwardQueueHead(head, tail, removedIndexes);
        entry.setPathValue(HEAD_PATH, result.getHeadIndex());

        // update removed indexes set if it was changed
        if (result.getRemovedIndexesChanged()) {
            entry.setPathValue(REMOVED_INDEXES_PATH, removedIndexes);
            entry.setPathValue(REMOVED_INDEXES_SIZE_PATH, removedIndexes.size());
        }

        return result;
    }

    @Override
    public String getName() {
        return "poll";
    }
}