package org.openspaces.collections.queue.distributed.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.HashSet;
import java.util.Set;

import static org.openspaces.collections.queue.distributed.data.DistrQueueMetadata.*;

/**
 * @author Oleksiy_Dyagilev
 */
public class DistrRemoveOperation extends CustomChangeOperation {

    private long index;

    public DistrRemoveOperation(long index) {
        this.index = index;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object change(MutableServerEntry entry) {
        long tail = (long) entry.getPathValue(TAIL_PATH);
        long head = (long) entry.getPathValue(HEAD_PATH);
        HashSet<Long> removedIndexes = (HashSet<Long>)((HashSet<Long>) entry.getPathValue(REMOVED_INDEXES_PATH)).clone();

        if (head == tail) {
            // nothing to remove
        } else if (index >= head && index <= tail) {
            removedIndexes.add(index);
            entry.setPathValue(REMOVED_INDEXES_PATH, removedIndexes);
            entry.setPathValue(REMOVED_INDEXES_SIZE_PATH, removedIndexes.size());
        }
        return null;
    }

    @Override
    public String getName() {
        return "remove";
    }
}
