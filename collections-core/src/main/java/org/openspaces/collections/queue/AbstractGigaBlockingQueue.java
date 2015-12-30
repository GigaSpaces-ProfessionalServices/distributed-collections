/**
 * 
 */
package org.openspaces.collections.queue;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;

import org.openspaces.collections.queue.data.QueueMetadata;
import org.openspaces.core.GigaSpace;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.query.aggregators.AggregationResult;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class AbstractGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    protected static final String NULL_ELEMENT_ERR_MSG = "Queue doesn't support null elements";
    protected static final String QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME = "Queue size change listener - ";

    protected final GigaSpace space;
    protected final String queueName;
    protected final boolean bounded;
    protected final int capacity;
    protected final ElementSerializer serializer;

    protected volatile boolean queueClosed;

    /**
     * Creates blocking queue
     *
     * @param space           giga space
     * @param queueName       unique queue queueName
     * @param capacity        queue capacity
     * @param bounded         flag whether queue is bounded
     */
    public AbstractGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded) {
        if (queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("'queueName' parameter must not be null or empty");
        }
        if (capacity < 0) {
            throw new IllegalArgumentException("'capacity' parameter must not be negative");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("'serializer' parameter must not be null");
        }
        this.space = requireNonNull(space, "'space' parameter must not be null");
        this.queueName = queueName;
        this.capacity = capacity;
        this.bounded = bounded;
        this.serializer = serializer;

        createNewMetadataIfRequired();
    }
    
    @Override
    public String getName() {
        return queueName;
    }
    
    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        requireNonNull(c, "Collection parameter must not be null");
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
    public boolean removeAll(Collection<?> c) {
        requireNonNull(c, "Collection parameter must not be null");
        return super.removeAll(c);
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        requireNonNull(c, "Collection parameter must not be null");
        return super.retainAll(c);
    }

    @Override
    public int remainingCapacity() {
        if (!bounded) {
            return Integer.MAX_VALUE;
        }

        return capacity - size();
    }

    protected abstract void createNewMetadataIfRequired();
 
    /**
     * extract single result from the aggregation result
     */
    @SuppressWarnings("unchecked")
    protected static <T extends Serializable> T toSingleResult(AggregationResult aggregationResult) {
        if (aggregationResult.size() != 1) {
            throw new IllegalStateException("Unexpected aggregation result size: " + aggregationResult.size());
        }

        return (T) aggregationResult.get(0);
    }

    /**
     * extract single result from the generic change api result
     */
    @SuppressWarnings("unchecked")
    protected <T extends Serializable> T toSingleResult(ChangeResult<QueueMetadata> changeResult) {
        if (changeResult.getNumberOfChangedEntries() == 0 && queueClosed){
            throw new IllegalStateException("Queue has been closed(deleted) from the grid: " + queueName);
        } else if (changeResult.getNumberOfChangedEntries() > 1) {
            throw new IllegalStateException("Unexpected number of changed entries: " + changeResult.getNumberOfChangedEntries());
        }

        return (T) changeResult.getResults().iterator().next().getChangeOperationsResults().iterator().next().getResult();
    }

    protected Object serialize(E element) {
        return serializer.serialize(element);
    }

    @SuppressWarnings("unchecked")
    protected E deserialize(Object payload) {
        return (E) serializer.deserialize(payload);
    }

}
