package org.openspaces.collections.queue.embedded;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.AbstractGigaBlockingQueue;
import org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer;
import org.openspaces.collections.queue.embedded.operations.*;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.QUEUE_NAME_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * Blocking queue implementation, that supports embedded collocation mode. 
 * 
 * @author Svitlana_Pogrebna
 */
public class EmbeddedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E, EmbeddedQueueContainer> {

    /**
     * Creates not bounded queue
     * 
     * @param space      space used to hold queue
     * @param queueName  unique queue name
     * @param serializer element serializer/deserializer
     */
    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, ElementSerializer<E> serializer) {
        super(space, queueName, 0, false, serializer);
    }

    
    /**
     * Creates bounded queue
     * @param space      space used to hold queue
     * @param queueName  unique queue name
     * @param capacity   queue capacity
     * @param serializer element serializer/deserializer
     */
    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, ElementSerializer<E> serializer) {
        super(space, queueName, capacity, true, serializer);
    }

    @Override
    protected EmbeddedSizeChangeListener createSizeChangeListener(EmbeddedQueueContainer queueMetadata) {
        return new EmbeddedSizeChangeListener(queueMetadata);
    }
    
    @Override
    public boolean offer(E e) {
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedOfferOperation(serialize(e)));

        final ChangeResult<EmbeddedQueueContainer> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);

        final SerializableResult<Boolean> result = toSingleResult(changeResult);

        return result.getResult();
    }

    @Override
    public E poll() {
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedPollOperation());

        final ChangeResult<EmbeddedQueueContainer> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);

        final SerializableResult<byte[]> result = toSingleResult(changeResult);
        return deserialize(result.getResult());
    }

    @Override
    public E peek() {
        final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(new EmbeddedPeekOperation()));

        final SerializableResult<byte[]> result = toSingleResult(aggregationResult);
        return deserialize(result.getResult());
    }

    @Override
    public CollocationMode getCollocationMode() {
        return CollocationMode.EMBEDDED;
    }

    @Override
    protected EmbeddedQueueContainer getOrCreate() {
        try {
            List<byte[]> items = bounded ? new ArrayList<byte[]>(capacity) : new ArrayList<byte[]>();
            EmbeddedQueueContainer container = new EmbeddedQueueContainer(queueName, items, bounded ? capacity : null);
            space.write(container, WriteModifiers.WRITE_ONLY);
            return container;
        } catch (EntryAlreadyInSpaceException e) {
            return getLightweightContainer();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new QueueIterator();
    }

    @Override
    public int size() {
        return getLightweightContainer().getSize();
    }

    private EmbeddedQueueContainer getLightweightContainer() {
        final IdQuery<EmbeddedQueueContainer> idQuery = idQuery().setProjections(SIZE_PATH);
        return space.read(idQuery);
    }
    
    private IdQuery<EmbeddedQueueContainer> idQuery() {
        return new IdQuery<EmbeddedQueueContainer>(EmbeddedQueueContainer.class, queueName);
    }

    @Override
    public void close() throws Exception {
        super.close();
        
        space.clear(idQuery());
    }

    private class QueueIterator implements Iterator<E> {

        private static final int DEFAULT_MAX_ENTRIES = 100;

        private final int maxEntries; 
        
        private byte[] curr;
        private int currIndex;
        
        private int batchIndex;
        private Iterator<byte[]> iterator;

        public QueueIterator() {
            this(DEFAULT_MAX_ENTRIES);
        }

        public QueueIterator(int maxEntries) {
            if (maxEntries <= 0) {
                throw new IllegalArgumentException("'maxEntries' parameter must be positive");
            }
            this.maxEntries = maxEntries;
            this.currIndex = -1;
            
            // load the first batch
            this.iterator = nextBatch();
        }

        @Override
        public boolean hasNext() {
            return iterator != null && iterator.hasNext();
        }

        @Override
        public E next() {
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            this.curr = iterator.next();
            currIndex++;

            if (!hasNext()) {
                iterator = nextBatch();
            }

            return deserialize(curr);
        }

        private Iterator<byte[]> nextBatch() {
            final EmbeddedRetrieveOperation operation = new EmbeddedRetrieveOperation(batchIndex, maxEntries);
            final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(operation));

            final SerializableResult<List<byte[]>> result = toSingleResult(aggregationResult);
            final List<byte[]> items = result.getResult();
            if (items == null) {
                return null;
            }

            batchIndex += items.size();
            return items.iterator();
        }

        @Override
        public void remove() {
            if (curr == null) {
                throw new IllegalStateException();
            }
            final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedRemoveOperation(currIndex, curr));
            space.change(idQuery(), changeSet);

            batchIndex--;
            currIndex--;
            curr = null;
        }
    }
    
    private class EmbeddedSizeChangeListener extends AbstractSizeChangeListener {

        public EmbeddedSizeChangeListener(EmbeddedQueueContainer container) {
            super(container);
        }

        @Override
        protected SQLQuery<EmbeddedQueueContainer> query() {
            SQLQuery<EmbeddedQueueContainer> query = new SQLQuery<>(EmbeddedQueueContainer.class, QUEUE_NAME_PATH + " = ? AND " + SIZE_PATH + " <> ?");
            query.setProjections(SIZE_PATH);
            return query;
        }

        @Override
        protected void populateParams(SQLQuery<EmbeddedQueueContainer> query) {
            query.setParameters(queueName, queueMetadata.getSize());
        }
    }
}
