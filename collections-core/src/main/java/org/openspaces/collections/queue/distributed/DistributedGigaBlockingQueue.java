package org.openspaces.collections.queue.distributed;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.AbstractGigaBlockingQueue;
import org.openspaces.collections.queue.distributed.data.DistrQueueMetadata;
import org.openspaces.collections.queue.distributed.data.DistrQueueItem;
import org.openspaces.collections.queue.distributed.data.DistrQueueItemKey;
import org.openspaces.collections.queue.distributed.operations.*;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.util.Pair;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.gigaspaces.client.ChangeModifiers.RETURN_DETAILED_RESULTS;
import static java.util.Objects.requireNonNull;
import static org.springframework.util.Assert.notNull;

/**
 * Blocking queue implementation that supports distributed and local collocation modes.
 * <p/>
 * Queue elements are stored by index. The algorithm doesn't require the use of transactions.
 *
 * @author Oleksiy_Dyagilev
 */
public class DistributedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E, DistrQueueMetadata> {
    private static final long WAIT_ITEM_TIMEOUT_MS = 5000;

    private final CollocationMode collocationMode;

    private final IdQuery<DistrQueueMetadata> queueMetadataQuery;

    /**
     * Creates not bounded queue
     *
     * @param space           space used to hold queue
     * @param queueName       unique queue name
     * @param collocationMode collocation mode
     */
    public DistributedGigaBlockingQueue(GigaSpace space, String queueName, CollocationMode collocationMode, ElementSerializer<E> serializer) {
        this(space, queueName, 0, false, collocationMode, serializer);
    }

    /**
     * Creates bounded queue
     *
     * @param space           space used to hold queue
     * @param queueName       unique queue name
     * @param capacity        queue capacity
     * @param collocationMode collocation mode
     */
    public DistributedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, CollocationMode collocationMode, ElementSerializer<E> serializer) {
        this(space, queueName, capacity, true, collocationMode, serializer);
    }

    /**
     * Creates blocking queue
     *
     * @param queueName       unique queue queueName
     * @param capacity        queue capacity
     * @param bounded         flag whether queue is bounded
     * @param collocationMode collocation mode
     */
    private DistributedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded, CollocationMode collocationMode, ElementSerializer<E> serializer) {
        super(space, queueName, capacity, bounded, serializer);
        notNull(collocationMode, "Collocation mode is null");
        if (collocationMode != CollocationMode.LOCAL && collocationMode != CollocationMode.DISTRIBUTED) {
            throw new IllegalArgumentException("Invalid collocation mode = " + collocationMode);
        }
        this.collocationMode = collocationMode;
        this.queueMetadataQuery = new IdQuery<>(DistrQueueMetadata.class, queueName);
    }
    
    @Override
    protected SizeChangeListener createSizeChangeListener(DistrQueueMetadata queueMetadata) {
        return new SizeChangeListener(queueMetadata);
    }

    @Override
    public boolean offer(E element) {
        requireNonNull(element, NULL_ELEMENT_ERR_MSG);

        ChangeSet offerChange = new ChangeSet().custom(new DistrOfferOperation(1));

        ChangeResult<DistrQueueMetadata> changeResult = space.change(queueMetadataQuery, offerChange, RETURN_DETAILED_RESULTS);

        DistrOfferOperation.Result offerResult = toSingleResult(changeResult);

        if (offerResult.isChanged()) {
            DistrQueueItemKey itemKey = new DistrQueueItemKey(queueName, offerResult.getNewTail());
            Integer routing = calculateRouting(itemKey);
            DistrQueueItem item = new DistrQueueItem(itemKey, serialize(element), routing);
            space.write(item);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public E poll() {
        while (true) {
            ChangeSet pollChange = new ChangeSet().custom(new DistrPollOperation());

            ChangeResult<DistrQueueMetadata> changeResult = space.change(queueMetadataQuery, pollChange, RETURN_DETAILED_RESULTS);

            DistrQueueHeadResult pollResult = toSingleResult(changeResult);

            if (pollResult.isQueueEmpty()) {
                return null;
            } else {
                Long polledIndex = pollResult.getHeadIndex();
                IdQuery<DistrQueueItem> itemQuery = itemQueryByIndex(polledIndex);

                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                DistrQueueItem queueItem = space.takeById(itemQuery, WAIT_ITEM_TIMEOUT_MS);

                // we may not find item if producer failed in the middle of modifying tail and writing item to space
                // just skip that item and poll the next one (see while loop)
                if (queueItem != null) {
                    return deserialize(queueItem.getItem());
                }
            }
        }
    }

    @Override
    public E peek() {
        while (true) {
            AggregationSet peekOperation = new AggregationSet().add(new DistrPeekOperation());

            AggregationResult aggregationResult = space.aggregate(queueMetadataQuery, peekOperation);
            DistrQueueHeadResult result = toSingleResult(aggregationResult);

            if (result.isQueueEmpty()) {
                return null;
            } else {
                Long headIndex = result.getHeadIndex();
                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                DistrQueueItem queueItem = space.readById(itemQueryByIndex(headIndex), WAIT_ITEM_TIMEOUT_MS);

                // we may not find item if producer failed in the middle of modifying tail and writing item to space
                // just skip that item and peek the next one (see while loop)
                if (queueItem != null) {
                    return deserialize(queueItem.getItem());
                }
            }
        }
    }

    @Override
    public Iterator<E> iterator() {
        DistrQueueMetadata queueMetadata = space.readById(queueMetadataQuery);
        return new QueueIterator(queueMetadata.getHead(), queueMetadata.getTail(), queueMetadata.getRemovedIndexes());
    }

    @Override
    public int size() {
        AggregationSet sizeOperation = new AggregationSet().add(new DistrSizeOperation());

        AggregationResult aggregationResult = space.aggregate(queueMetadataQuery, sizeOperation);

        DistrSizeOperation.Result sizeResult = toSingleResult(aggregationResult);
        return sizeResult.getSize();
    }

    /**
     * create new queue in the grid if this is a first reference (queue doesn't exist yet)
     */
    @Override
    protected DistrQueueMetadata getOrCreate() {
        try {
            DistrQueueMetadata queueMetadata = new DistrQueueMetadata(queueName, 0L, 0L, bounded, capacity);
            space.write(queueMetadata, WriteModifiers.WRITE_ONLY);
            return queueMetadata;
        } catch (EntryAlreadyInSpaceException e) {
            return space.read(queueMetadataQuery);
        }
    }

    /**
     * @return query to find item by index
     */
    private IdQuery<DistrQueueItem> itemQueryByIndex(long index) {
        DistrQueueItemKey itemKey = new DistrQueueItemKey(queueName, index);
        return new IdQuery<>(DistrQueueItem.class, itemKey, calculateRouting(itemKey));
    }

    /**
     * @return template to find item by index
     */
    private DistrQueueItem itemTemplateByIndex(long index) {
        DistrQueueItem itemTemplate = new DistrQueueItem();
        DistrQueueItemKey itemKey = new DistrQueueItemKey(queueName, index);
        itemTemplate.setItemKey(itemKey);
        itemTemplate.setRouting(calculateRouting(itemKey));
        return itemTemplate;
    }

    private class QueueIterator implements Iterator<E> {

        private Long currIndex;
        private Long nextIndex;
        private Long endIndex;
        private E next;
        private E curr;
        private Set<Long> removedIndexes;

        public QueueIterator(Long head, Long tail, Set<Long> removedIndexes) {
            this.currIndex = head;
            this.endIndex = tail;
            this.removedIndexes = new HashSet<>(removedIndexes);

            if (currIndex < endIndex) {
                Pair<E, Long> itemAndIndex = readItemByIndex(currIndex + 1);
                next = itemAndIndex.getFirst();
                nextIndex = itemAndIndex.getSecond();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            curr = next;
            currIndex = nextIndex;

            if (currIndex < endIndex) {
                Pair<E, Long> itemAndIndex = readItemByIndex(currIndex + 1);
                next = itemAndIndex.getFirst();
                nextIndex = itemAndIndex.getSecond();
            } else {
                next = null;
            }

            return curr;
        }

        @Override
        public void remove() {
            if (curr == null) {
                throw new IllegalStateException();
            }

            // update queue
            ChangeSet removeOperation = new ChangeSet().custom(new DistrRemoveOperation(currIndex));
            space.change(queueMetadataQuery, removeOperation);

            // remove element
            space.clear(itemTemplateByIndex(currIndex));
            
            curr = null;
        }

        private Pair<E, Long> readItemByIndex(long index) {
            while (index <= endIndex) {
                if (removedIndexes.remove(index)) {
                    index++;
                    continue;
                }

                IdQuery<DistrQueueItem> itemQuery = itemQueryByIndex(index);
                DistrQueueItem queueItem = space.readById(itemQuery);
                if (queueItem != null) {
                    return new Pair<>(deserialize(queueItem.getItem()), index);
                } else {
                    index++;
                }
            }
            return new Pair<>(null, null);
        }
    }

    private class SizeChangeListener extends AbstractSizeChangeListener {

        public SizeChangeListener(DistrQueueMetadata queueMetadata) {
            super(queueMetadata);
        }

        @Override
        protected SQLQuery<DistrQueueMetadata> query() {
            SQLQuery<DistrQueueMetadata> query = new SQLQuery<>(DistrQueueMetadata.class, "name = ? AND (tail <> ? OR head <> ? OR removedIndexesSize <> ?)");
            query.setProjections("tail", "head", "removedIndexesSize");
            return query;
        }

        @Override
        protected void populateParams(SQLQuery<DistrQueueMetadata> query) {
            query.setParameters(queueName, queueMetadata.getTail(), queueMetadata.getHead(), queueMetadata.getRemovedIndexesSize());
        }
    }

    private Integer calculateRouting(DistrQueueItemKey itemKey) {
        switch (collocationMode) {
            case LOCAL:
                return itemKey.getQueueName().hashCode();
            case DISTRIBUTED:
                return itemKey.hashCode();
            default:
                throw new UnsupportedOperationException("Invalid collocation mode = " + collocationMode);
        }
    }

    @Override
    public CollocationMode getCollocationMode() {
        return collocationMode;
    }

    @Override
    public void close() throws Exception {
        super.close();
        
        space.clear(queueMetadataQuery);

        // cannot use space.clear() because we need to match by nested template
        SQLQuery<DistrQueueItem> itemQuery = new SQLQuery<>(DistrQueueItem.class, "itemKey.queueName = ?");
        itemQuery.setParameters(queueName);
        itemQuery.setProjections("");
        space.takeMultiple(itemQuery);
    }
}
