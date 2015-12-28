package org.openspaces.collections.queue;

import static com.gigaspaces.client.ChangeModifiers.RETURN_DETAILED_RESULTS;
import static org.openspaces.collections.queue.data.QueueMetadata.REMOVED_INDEXES_PATH;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.QueueMetadata;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.queue.data.QueueItemKey;
import org.openspaces.collections.queue.operations.OfferOperation;
import org.openspaces.collections.queue.operations.PeekOperation;
import org.openspaces.collections.queue.operations.PollOperation;
import org.openspaces.collections.queue.operations.QueueHeadResult;
import org.openspaces.collections.queue.operations.SizeOperation;
import org.openspaces.collections.util.Pair;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;

/**
 * @author Oleksiy_Dyagilev
 */
public class DefaultGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    private static final long WAIT_ITEM_TIMEOUT_MS = 5000;

    private final GigaSpace space;
    private final String queueName;
    private final boolean bounded;
    private final int capacity;
    private final CollocationMode collocationMode;

    /**
     * Creates not bounded queue
     *
     * @param space
     * @param queueName
     * @param collocationMode
     */
    public DefaultGigaBlockingQueue(GigaSpace space, String queueName, CollocationMode collocationMode) {
        this(space, queueName, 0, false, collocationMode);
    }

    /**
     * Creates bounded queue
     *
     * @param space
     * @param queueName
     * @param capacity
     * @param collocationMode
     */
    public DefaultGigaBlockingQueue(GigaSpace space, String queueName, int capacity, CollocationMode collocationMode) {
        this(space, queueName, capacity, true, collocationMode);
    }

    /**
     * Creates blocking queue
     *
     * @param queueName       unique queue queueName
     * @param capacity        queue capacity
     * @param bounded         flag whether queue is bounded
     * @param collocationMode collocation mode
     */
    private DefaultGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded, CollocationMode collocationMode) {
        if (queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("'queueName' parameter must not be null or empty");
        }
        if (capacity < 0) {
            throw new IllegalArgumentException("'capacity' parameter must not be negative");
        }
        this.space = Objects.requireNonNull(space, "'space' parameter must not be null");
        this.queueName = queueName;
        this.capacity = capacity;
        this.bounded = bounded;
        this.collocationMode = Objects.requireNonNull(collocationMode, "'collocationMode' parameter must not be null");

        createNewIfRequired();
    }

    @Override
    public boolean offer(E element) {
        ChangeSet offerChange = new ChangeSet().custom(new OfferOperation(1));

        ChangeResult<QueueMetadata> changeResult = space.change(queueQuery(), offerChange, RETURN_DETAILED_RESULTS);

        OfferOperation.Result offerResult = toSingleResult(changeResult);

        if (offerResult.isChanged()) {
            QueueItemKey itemKey = new QueueItemKey(queueName, offerResult.getNewTail());
            Integer routing = calculateRouting(itemKey);
            QueueItem<E> item = new QueueItem<>(itemKey, element, routing);
            space.write(item);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void put(E element) throws InterruptedException {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public E take() throws InterruptedException {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public int remainingCapacity() {
        if (!bounded) {
            return Integer.MAX_VALUE;
        }

        return capacity - size();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        checkNotNull(c);
        if (c == this) {
            throw new IllegalArgumentException();
        }
        if (maxElements <= 0) {
            return 0;
        }

        int max = Math.min(size(), maxElements);

        for (int i = 0; i < max; i++) {
            E element = poll();
            c.add(element);
        }

        return max;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        while (true) {
            ChangeSet pollChange = new ChangeSet().custom(new PollOperation());

            ChangeResult<QueueMetadata> changeResult = space.change(queueQuery(), pollChange, RETURN_DETAILED_RESULTS);

            QueueHeadResult pollResult = toSingleResult(changeResult);

            if (pollResult.isQueueEmpty()) {
                return null;
            } else {
                Long polledIndex = pollResult.getHeadIndex();
                IdQuery<QueueItem> itemQuery = itemQueryByIndex(polledIndex);

                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                QueueItem<E> queueItem = space.takeById(itemQuery, WAIT_ITEM_TIMEOUT_MS);

                // we may not find item if producer failed in the middle of modifying tail and writing item to space
                // just skip that item and poll the next one (see while loop)
                if (queueItem != null) {
                    return queueItem.getItem();
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E peek() {
        while (true) {
            AggregationSet peekOperation = new AggregationSet().add(new PeekOperation());

            AggregationResult aggregationResult = space.aggregate(queueQuery(), peekOperation);
            QueueHeadResult result = toSingleResult(aggregationResult);

            if (result.isQueueEmpty()) {
                return null;
            } else {
                Long headIndex = result.getHeadIndex();
                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                QueueItem<E> queueItem = space.readById(itemQueryByIndex(headIndex), WAIT_ITEM_TIMEOUT_MS);

                // we may not find item if producer failed in the middle of modifying tail and writing item to space
                // just skip that item and peek the next one (see while loop)
                if (queueItem != null) {
                    return queueItem.getItem();
                }
            }
        }
    }

    @Override
    public Iterator<E> iterator() {
        QueueMetadata queueMetadata = space.readById(queueQuery());
        return new QueueIterator(queueMetadata.getHead(), queueMetadata.getTail(), queueMetadata.getRemovedIndexes());
    }

    @Override
    public int size() {
        AggregationSet sizeOperation = new AggregationSet().add(new SizeOperation());

        AggregationResult aggregationResult = space.aggregate(queueQuery(), sizeOperation);

        SizeOperation.Result sizeResult = toSingleResult(aggregationResult);
        return sizeResult.getSize();
    }

    /**
     * Throws NullPointerException if argument is null.
     *
     * @param v the element
     */
    private static void checkNotNull(Object v) {
        if (v == null) {
            throw new NullPointerException();
        }
    }

    /**
     * create new queue in the grid if this is a first reference (queue doesn't exist yet)
     */
    private void createNewIfRequired() {
        try {
            QueueMetadata queueMetadata = new QueueMetadata(queueName, 0L, 0L, bounded, capacity);
            space.write(queueMetadata, WriteModifiers.WRITE_ONLY);
        } catch (EntryAlreadyInSpaceException e) {
            // no-op
        }
    }

    /**
     * @return query to find queue by unique id (name)
     */
    private IdQuery<QueueMetadata> queueQuery() {
        return new IdQuery<>(QueueMetadata.class, queueName);
    }

    /**
     * @return query to find item by index
     */
    private IdQuery<QueueItem> itemQueryByIndex(long index) {
        QueueItemKey itemKey = new QueueItemKey(queueName, index);
        return new IdQuery<>(QueueItem.class, itemKey, calculateRouting(itemKey));
    }

    /**
     * @return template to find item by index
     */
    private QueueItem<?> itemTemplateByIndex(long index) {
        QueueItem<?> itemTemplate = new QueueItem<>();
        QueueItemKey itemKey = new QueueItemKey(queueName, index);
        itemTemplate.setItemKey(itemKey);
        itemTemplate.setRouting(calculateRouting(itemKey));
        return itemTemplate;
    }

    /**
     * extract single result from the aggregation result
     */
    @SuppressWarnings("unchecked")
    private <T extends Serializable> T toSingleResult(AggregationResult aggregationResult) {
        if (aggregationResult.size() != 1) {
            throw new IllegalStateException("Unexpected aggregation result size: " + aggregationResult.size());
        }

        return (T) aggregationResult.get(0);
    }

    /**
     * extract single result from the generic change api result
     */
    @SuppressWarnings("unchecked")
    private <T extends Serializable> T toSingleResult(ChangeResult<QueueMetadata> changeResult) {
        if (changeResult.getNumberOfChangedEntries() != 1) {
            throw new IllegalStateException("Unexpected number of changed entries: " + changeResult.getNumberOfChangedEntries());
        }

        return (T) changeResult.getResults().iterator().next().getChangeOperationsResults().iterator().next().getResult();
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

            ChangeSet removeOperation = new ChangeSet().addAllToCollection(REMOVED_INDEXES_PATH, currIndex);

            // update queue
            space.change(queueQuery(), removeOperation);

            // remove element
            space.clear(itemTemplateByIndex(currIndex));
        }

        @SuppressWarnings("unchecked")
        private Pair<E, Long> readItemByIndex(long index) {
            while (index <= endIndex) {
                if (removedIndexes.remove(index)) {
                    index++;
                    continue;
                }

                IdQuery<QueueItem> itemQuery = itemQueryByIndex(index);
                QueueItem<E> queueItem = space.readById(itemQuery, WAIT_ITEM_TIMEOUT_MS);
                if (queueItem != null) {
                    return new Pair<>(queueItem.getItem(), index);
                } else {
                    index++;
                }
            }
            return new Pair<>(null, index);
        }
    }

    private Integer calculateRouting(QueueItemKey itemKey) {
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
    public String getName() {
        return queueName;
    }

    @Override
    public CollocationMode getCollocationMode() {
        return collocationMode;
    }

    @Override
    public void close() throws Exception {
        // TODO: implement removing the queue
    }
}
