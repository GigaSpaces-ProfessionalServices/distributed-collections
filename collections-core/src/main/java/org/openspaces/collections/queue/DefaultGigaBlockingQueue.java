package org.openspaces.collections.queue;

import static com.gigaspaces.client.ChangeModifiers.RETURN_DETAILED_RESULTS;
import static org.openspaces.collections.queue.data.QueueData.REMOVED_INDEXES_PATH;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.openspaces.collections.queue.data.QueueData;
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
import com.j_spaces.core.client.SQLQuery;

/**
 * @author Oleksiy_Dyagilev
 */
public class DefaultGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    private static final long WAIT_ITEM_TIMEOUT_MS = 5000;

    private GigaSpace space;
    private String queueName;
    private boolean bounded;
    private int capacity;

    /**
     * Create not bounded queue
     *
     * @param queueName unique queue queueName
     */
    public DefaultGigaBlockingQueue(GigaSpace space, String queueName) {
        this.space = space;
        this.queueName = queueName;
        this.bounded = false;

        createNewIfRequired();
    }

    /**
     * Create bounded queue
     *
     * @param queueName unique queue queueName
     * @param capacity  queue capacity
     */
    public DefaultGigaBlockingQueue(GigaSpace space, String queueName, int capacity) {
        this.space = space;
        this.queueName = queueName;
        this.capacity = capacity;
        this.bounded = true;

        createNewIfRequired();
    }

    @Override
    public boolean offer(E element) {
        ChangeSet offerChange = new ChangeSet().custom(new OfferOperation(1));

        ChangeResult<QueueData> changeResult = space.change(queueQuery(), offerChange, RETURN_DETAILED_RESULTS);

        OfferOperation.Result offerResult = toSingleResult(changeResult);

        if (offerResult.isChanged()) {
            QueueItemKey itemKey = new QueueItemKey(queueName, offerResult.getNewTail());
            QueueItem<E> item = new QueueItem<>(itemKey, element);
            space.write(item);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
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
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        while (true) {
            ChangeSet pollChange = new ChangeSet().custom(new PollOperation());

            ChangeResult<QueueData> changeResult = space.change(queueQuery(), pollChange, RETURN_DETAILED_RESULTS);

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
        QueueData queueData = space.readById(queueQuery());
        return new QueueIterator(queueData.getHead(), queueData.getTail());
    }

    @Override
    public int size() {
        AggregationSet sizeOperation = new AggregationSet().add(new SizeOperation());
        
        AggregationResult aggregationResult = space.aggregate(queueQuery(), sizeOperation);

        SizeOperation.Result sizeResult = toSingleResult(aggregationResult);
        return sizeResult.getSize();
    }

    /**
     * create new queue in the grid if this is a first reference (queue doesn't exist yet)
     */
    private void createNewIfRequired() {
        try {
            QueueData queueData = new QueueData(queueName, 0L, 0L, bounded, capacity);
            space.write(queueData, WriteModifiers.WRITE_ONLY);
        } catch (EntryAlreadyInSpaceException e) {
            // no-op
        }
    }

    /**
     * @return query to find queue by unique id (name)
     */
    private IdQuery<QueueData> queueQuery() {
        return new IdQuery<>(QueueData.class, queueName);
    }
    
    /**
     * @return query to find item by index
     */
    private IdQuery<QueueItem> itemQueryByIndex(long index) {
        QueueItemKey itemKey = new QueueItemKey(queueName, index);
        return new IdQuery<>(QueueItem.class, itemKey);
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
    private <T extends Serializable> T toSingleResult(ChangeResult<QueueData> changeResult) {
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

        public QueueIterator(Long head, Long tail) {
            this.currIndex = head;
            this.endIndex = tail;

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
            QueueItem<Object> itemTemplate = new QueueItem<>();
            itemTemplate.setItemKey(new QueueItemKey(queueName, currIndex));
            space.clear(itemTemplate);
        }

        @SuppressWarnings("unchecked")
        private Pair<E, Long> readItemByIndex(long index) {
            while (index <= endIndex) {
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
}
