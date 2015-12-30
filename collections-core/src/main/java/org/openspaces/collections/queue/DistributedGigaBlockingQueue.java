package org.openspaces.collections.queue;

import static com.gigaspaces.client.ChangeModifiers.RETURN_DETAILED_RESULTS;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.springframework.util.Assert.notNull;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.queue.data.QueueItemKey;
import org.openspaces.collections.queue.data.QueueMetadata;
import org.openspaces.collections.queue.operations.OfferOperation;
import org.openspaces.collections.queue.operations.PeekOperation;
import org.openspaces.collections.queue.operations.PollOperation;
import org.openspaces.collections.queue.operations.QueueHeadResult;
import org.openspaces.collections.queue.operations.RemoveOperation;
import org.openspaces.collections.queue.operations.SizeOperation;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.util.MiscUtils;
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
public class DistributedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E> {
    private static final long WAIT_ITEM_TIMEOUT_MS = 5000;
    private static final long SIZE_CHANGE_LISTENER_TIMEOUT_MS = 5000;

    private final CollocationMode collocationMode;

    private final Semaphore readSemaphore;
    private final Semaphore writeSemaphore;

    private final Thread sizeChangeListenerThread;

    private final IdQuery<QueueMetadata> queueMetadataQuery;

    /**
     * Creates not bounded queue
     *
     * @param space           space used to hold queue
     * @param queueName       unique queue name
     * @param collocationMode collocation mode
     */
    public DistributedGigaBlockingQueue(GigaSpace space, String queueName, CollocationMode collocationMode, ElementSerializer serializer) {
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
    public DistributedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, CollocationMode collocationMode, ElementSerializer serializer) {
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
    private DistributedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded, CollocationMode collocationMode, ElementSerializer serializer) {
        super(space, queueName, capacity, bounded, serializer);
        notNull(collocationMode, "Collocation mode is null");
        if (collocationMode != CollocationMode.LOCAL && collocationMode != CollocationMode.DISTRIBUTED) {
            throw new IllegalArgumentException("Invalid collocation mode = " + collocationMode);
        }
        this.collocationMode = collocationMode;
        this.queueMetadataQuery = new IdQuery<>(QueueMetadata.class, queueName);

        // setup semaphores
        QueueMetadata metadata = space.read(queueMetadataQuery);
        long tail = metadata.getTail();
        long head = metadata.getHead();
        int removedIndexesSize = metadata.getRemovedIndexesSize();
        int size = (int) (tail - head - removedIndexesSize);


        this.readSemaphore = new Semaphore(size, true);
        this.writeSemaphore = bounded ? new Semaphore(capacity - size) : null;

        // start size change listener thread
        SizeChangeListener sizeChangeListener = new SizeChangeListener(tail, head, removedIndexesSize);
        this.sizeChangeListenerThread = new Thread(sizeChangeListener, QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME + queueName);
        this.sizeChangeListenerThread.start();
    }

    @Override
    public boolean offer(E element) {
        requireNonNull(element, NULL_ELEMENT_ERR_MSG);

        ChangeSet offerChange = new ChangeSet().custom(new OfferOperation(1));

        ChangeResult<QueueMetadata> changeResult = space.change(queueMetadataQuery, offerChange, RETURN_DETAILED_RESULTS);

        OfferOperation.Result offerResult = toSingleResult(changeResult);

        if (offerResult.isChanged()) {
            QueueItemKey itemKey = new QueueItemKey(queueName, offerResult.getNewTail());
            Integer routing = calculateRouting(itemKey);
            QueueItem item = new QueueItem(itemKey, serialize(element), routing);
            space.write(item);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void put(E element) throws InterruptedException {
        if (!bounded) {
            offer(element);
            return;
        }

        while (true) {
            writeSemaphore.acquire();
            if (offer(element)) {
                return;
            }
        }
    }

    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        if (!bounded) {
            return offer(element);
        }

        long endTime = currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (currentTimeMillis() < endTime) {
            if (writeSemaphore.tryAcquire(timeout, unit)) {
                if (offer(element)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public E take() throws InterruptedException {
        while (true) {
            readSemaphore.acquire();
            E e = poll();
            if (e != null) {
                return e;
            }
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

        while (currentTimeMillis() < endTime) {
            if (readSemaphore.tryAcquire(timeout, unit)) {
                E e = poll();
                if (e != null) {
                    return e;
                }
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        while (true) {
            ChangeSet pollChange = new ChangeSet().custom(new PollOperation());

            ChangeResult<QueueMetadata> changeResult = space.change(queueMetadataQuery, pollChange, RETURN_DETAILED_RESULTS);

            QueueHeadResult pollResult = toSingleResult(changeResult);

            if (pollResult.isQueueEmpty()) {
                return null;
            } else {
                Long polledIndex = pollResult.getHeadIndex();
                IdQuery<QueueItem> itemQuery = itemQueryByIndex(polledIndex);

                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                QueueItem queueItem = space.takeById(itemQuery, WAIT_ITEM_TIMEOUT_MS);

                // we may not find item if producer failed in the middle of modifying tail and writing item to space
                // just skip that item and poll the next one (see while loop)
                if (queueItem != null) {
                    return deserialize(queueItem.getItem());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E peek() {
        while (true) {
            AggregationSet peekOperation = new AggregationSet().add(new PeekOperation());

            AggregationResult aggregationResult = space.aggregate(queueMetadataQuery, peekOperation);
            QueueHeadResult result = toSingleResult(aggregationResult);

            if (result.isQueueEmpty()) {
                return null;
            } else {
                Long headIndex = result.getHeadIndex();
                // there is a time window when queue tail changed, but item is not in the space yet
                // to handle that we do take with timeout
                QueueItem queueItem = space.readById(itemQueryByIndex(headIndex), WAIT_ITEM_TIMEOUT_MS);

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
        QueueMetadata queueMetadata = space.readById(queueMetadataQuery);
        return new QueueIterator(queueMetadata.getHead(), queueMetadata.getTail(), queueMetadata.getRemovedIndexes());
    }

    @Override
    public int size() {
        AggregationSet sizeOperation = new AggregationSet().add(new SizeOperation());

        AggregationResult aggregationResult = space.aggregate(queueMetadataQuery, sizeOperation);

        SizeOperation.Result sizeResult = toSingleResult(aggregationResult);
        return sizeResult.getSize();
    }

    /**
     * Set the proper number of permits for 'read' and 'write' semaphore based on the current state of metadata
     */
    private void onSizeChanged(long tail, long head, int removedIndexesSize) {
        int size = (int) (tail - head - removedIndexesSize);

        readSemaphore.drainPermits();
        if (size > 0) {
            readSemaphore.release(size);
        }

        if (bounded) {
            writeSemaphore.drainPermits();
            int availableSpace = capacity - size;
            if (availableSpace > 0) {
                writeSemaphore.release(availableSpace);
            }
        }
    }

    /**
     * create new queue in the grid if this is a first reference (queue doesn't exist yet)
     */
    @Override
    protected void createNewMetadataIfRequired() {
        try {
            QueueMetadata queueMetadata = new QueueMetadata(queueName, 0L, 0L, bounded, capacity);
            space.write(queueMetadata, WriteModifiers.WRITE_ONLY);
        } catch (EntryAlreadyInSpaceException e) {
            // no-op
        }
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
    private QueueItem itemTemplateByIndex(long index) {
        QueueItem itemTemplate = new QueueItem();
        QueueItemKey itemKey = new QueueItemKey(queueName, index);
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
            ChangeSet removeOperation = new ChangeSet().custom(new RemoveOperation(currIndex));
            space.change(queueMetadataQuery, removeOperation);

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
                QueueItem queueItem = space.readById(itemQuery);
                if (queueItem != null) {
                    return new Pair<>(deserialize(queueItem.getItem()), index);
                } else {
                    index++;
                }
            }
            return new Pair<>(null, null);
        }
    }

    private class SizeChangeListener implements Runnable {
        private Long tail;
        private Long head;
        private Integer removedIndexesSize;

        public SizeChangeListener(Long tail, Long head, Integer removedIndexesSize) {
            this.tail = tail;
            this.head = head;
            this.removedIndexesSize = removedIndexesSize;
        }

        @Override
        public void run() {
            SQLQuery<QueueMetadata> query = new SQLQuery<>(QueueMetadata.class, "name = ? AND (tail <> ? OR head <> ? OR removedIndexesSize <> ?)");
            query.setProjections("tail", "head", "removedIndexesSize");

            while (!queueClosed) {
                query.setParameters(queueName, tail, head, removedIndexesSize);
                try {
                    QueueMetadata foundMetadata = space.read(query, SIZE_CHANGE_LISTENER_TIMEOUT_MS);
                    if (foundMetadata != null) {
                        this.tail = foundMetadata.getTail();
                        this.head = foundMetadata.getHead();
                        this.removedIndexesSize = foundMetadata.getRemovedIndexesSize();

                        onSizeChanged(tail, head, removedIndexesSize);
                    }
                } catch (Exception e) {
                    if (isInterrupted(e) || isClosedResource(e)) {
                        return;
                    } else {
                        if (!queueClosed) {
                            throw e;
                        }
                    }
                }
            }
        }
    }

    private boolean isInterrupted(Throwable e) {
        return MiscUtils.hasCause(e, InterruptedException.class);
    }

    private boolean isClosedResource(Throwable e){
        return MiscUtils.hasCause(e, com.j_spaces.core.exception.ClosedResourceException.class);
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
    public CollocationMode getCollocationMode() {
        return collocationMode;
    }

    @Override
    public void close() throws Exception {
        this.queueClosed = true;
        this.sizeChangeListenerThread.interrupt();
        space.clear(queueMetadataQuery);

        // cannot use space.clear() because we need to match by nested template
        SQLQuery<QueueItem> itemQuery = new SQLQuery<>(QueueItem.class, "itemKey.queueName = ?");
        itemQuery.setParameters(queueName);
        itemQuery.setProjections("");
        space.takeMultiple(itemQuery);
    }
}
