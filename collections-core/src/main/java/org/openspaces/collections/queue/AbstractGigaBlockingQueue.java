/**
 *
 */
package org.openspaces.collections.queue;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.util.MiscUtils;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class AbstractGigaBlockingQueue<E, M extends QueueMetadata> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    protected static final String NULL_ELEMENT_ERR_MSG = "Queue doesn't support null elements";
  
    protected static final String QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME = "Queue size change listener - ";
    private static final long SIZE_CHANGE_LISTENER_TIMEOUT_MS = 5000;
    
    protected final GigaSpace space;
    protected final String queueName;
    protected final boolean bounded;
    protected final int capacity;
    protected final ElementSerializer serializer;

    protected final Semaphore readSemaphore;
    protected final Semaphore writeSemaphore;

    protected final Thread sizeChangeListenerThread;

    protected volatile boolean queueClosed;

    /**
     * Creates blocking queue
     *
     * @param space     giga space
     * @param queueName unique queue queueName
     * @param capacity  queue capacity
     * @param bounded   flag whether queue is bounded
     */
    public AbstractGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded, ElementSerializer serializer) {
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

        final M queueMetadata = getOrCreate();
        final int size = queueMetadata.getSize();

        this.readSemaphore = new Semaphore(size, true);
        this.writeSemaphore = bounded ? new Semaphore(capacity - size) : null;

        // start size change listener thread
        final Runnable sizeChangeListener = createSizeChangeListener(queueMetadata);
        this.sizeChangeListenerThread = new Thread(sizeChangeListener, QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME + queueName);
        this.sizeChangeListenerThread.start();
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
    
    protected abstract M getOrCreate();
    
    protected abstract AbstractSizeChangeListener createSizeChangeListener(M queueMetadata);
    
    /**
     * Set the proper number of permits for 'read' and 'write' semaphore based on the current queue size
     */
    protected void onSizeChanged(int size) {
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
     * extract single result from the aggregation result
     */
    @SuppressWarnings("unchecked")
    protected <T extends Serializable> T toSingleResult(AggregationResult aggregationResult) {
        if (aggregationResult.size() == 0 && queueClosed) {
            throw new IllegalStateException("Queue has been closed(deleted) from the grid: " + queueName);
        } else if (aggregationResult.size() != 1) {
            throw new IllegalStateException("Unexpected aggregation result size: " + aggregationResult.size());
        }

        return (T) aggregationResult.get(0);
    }

    /**
     * extract single result from the generic change api result
     */
    @SuppressWarnings("unchecked")
    protected <T extends Serializable> T toSingleResult(ChangeResult<?> changeResult) {
        if (changeResult.getNumberOfChangedEntries() == 0 && queueClosed) {
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

    @Override
    public void close() throws Exception {
        this.queueClosed = true;
        this.sizeChangeListenerThread.interrupt();
    }
    
    protected abstract class AbstractSizeChangeListener implements Runnable {

        protected final M queueMetadata;

        public AbstractSizeChangeListener(M queueMetadata) {
            this.queueMetadata = queueMetadata;
        }

        @Override
        public void run() {
            final SQLQuery<M> query = query();

            while (!queueClosed) {
                populateParams(query);
                try {
                    M foundMetadata = space.read(query, SIZE_CHANGE_LISTENER_TIMEOUT_MS);
                    if (foundMetadata != null) {
                        onSizeChanged(foundMetadata.getSize());
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

        protected abstract SQLQuery<M> query();

        protected abstract void populateParams(SQLQuery<M> query);

        private boolean isInterrupted(Throwable e) {
            return MiscUtils.hasCause(e, InterruptedException.class);
        }

        private boolean isClosedResource(Throwable e) {
            return MiscUtils.hasCause(e, com.j_spaces.core.exception.ClosedResourceException.class);
        }
    }
}
