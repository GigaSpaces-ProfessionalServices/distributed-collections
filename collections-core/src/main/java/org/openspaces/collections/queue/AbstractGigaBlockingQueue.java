/**
 * 
 */
package org.openspaces.collections.queue;

import static java.util.Objects.requireNonNull;

import java.util.AbstractQueue;
import java.util.Collection;

import org.openspaces.core.GigaSpace;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class AbstractGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    protected static final String NULL_ELEMENT_ERR_MSG = "Queue doesn't support null elements";

    protected final GigaSpace space;
    protected final String queueName;
    protected final boolean bounded;
    protected final int capacity;

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
        this.space = requireNonNull(space, "'space' parameter must not be null");
        this.queueName = queueName;
        this.capacity = capacity;
        this.bounded = bounded;

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
    
}
