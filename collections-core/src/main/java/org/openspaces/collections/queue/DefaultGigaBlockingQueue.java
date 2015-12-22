package org.openspaces.collections.queue;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import org.openspaces.collections.queue.data.QueueData;
import org.openspaces.collections.queue.data.QueueItem;
import org.openspaces.collections.queue.data.QueueItemKey;
import org.openspaces.collections.queue.operations.OfferOperation;
import org.openspaces.collections.queue.operations.PollOperation;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.gigaspaces.client.ChangeModifiers.RETURN_DETAILED_RESULTS;

/**
 * @author Oleksiy_Dyagilev
 */
public class DefaultGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

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

    @Override
    public boolean offer(E element) {
        ChangeSet offerChange = new ChangeSet().custom(new OfferOperation(1));

        ChangeResult<QueueData> changeResult = space.change(queueTemplate(), offerChange, RETURN_DETAILED_RESULTS);

        OfferOperation.Result offerResult = (OfferOperation.Result) toSingleResult(changeResult);

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
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new RuntimeException("Not implemented yet");
    }

    /**
     * @return template to query queue
     */
    private QueueData queueTemplate() {
        QueueData queueTemplate = new QueueData();
        queueTemplate.setName(queueName);
        return queueTemplate;
    }

    private Serializable toSingleResult(ChangeResult<QueueData> changeResult) {
        if (changeResult.getNumberOfChangedEntries() != 1) {
            throw new IllegalStateException("Unexpected number of changed entries: " + changeResult.getNumberOfChangedEntries());
        }

        return changeResult.getResults().iterator().next().getChangeOperationsResults().iterator().next().getResult();
    }

    @Override
    public E poll() {
        ChangeSet pollChange = new ChangeSet().custom(new PollOperation());

        ChangeResult<QueueData> changeResult = space.change(queueTemplate(), pollChange, RETURN_DETAILED_RESULTS);

        PollOperation.Result pollResult = (PollOperation.Result) toSingleResult(changeResult);

        if (pollResult.isQueueEmpty()) {
            return null;
        } else {
            Long polledIndex = pollResult.getPolledIndex();
            QueueItemKey itemKey = new QueueItemKey(queueName, polledIndex);
            QueueItem queueItem = space.takeById(QueueItem.class, itemKey);

            // TODO: race condition
            return queueItem == null ? null : (E) queueItem.getItem();
        }

    }

    @Override
    public E peek() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public int size() {
        throw new RuntimeException("Not implemented yet");
    }
}
