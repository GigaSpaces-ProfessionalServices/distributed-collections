/**
 *
 */
package org.openspaces.collections.queue.embedded;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.AbstractGigaBlockingQueue;
import org.openspaces.collections.queue.embedded.data.EmbeddedQueue;
import org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer;
import org.openspaces.collections.queue.embedded.operations.EmbeddedOfferOperation;
import org.openspaces.collections.queue.embedded.operations.EmbeddedPeekOperation;
import org.openspaces.collections.queue.embedded.operations.EmbeddedPollOperation;
import org.openspaces.collections.queue.embedded.operations.EmbeddedQueueChangeResult;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E> {

    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, ElementSerializer serializer) {
        super(space, queueName, 0, false, serializer);
    }

    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, ElementSerializer serializer) {
        super(space, queueName, capacity, true, serializer);
    }

    @Override
    public boolean offer(E e) {
        // TODO: add serialization
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedOfferOperation(e));

        final ChangeResult<EmbeddedQueue> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);

        final EmbeddedQueueChangeResult<Boolean> result = toSingleResult(changeResult);

        return result.getResult();
    }

    @Override
    public void put(E e) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E take() throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @SuppressWarnings("unchecked")
    @Override
    public E poll() {
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedPollOperation());

        final ChangeResult<EmbeddedQueue> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);

        final EmbeddedQueueChangeResult<Object> result = toSingleResult(changeResult);
        // TODO: add deserialization
        return (E) result.getResult();
    }

    @SuppressWarnings("unchecked")
    @Override
    public E peek() {
        final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(new EmbeddedPeekOperation()));

        final EmbeddedQueueChangeResult<Object> result = toSingleResult(aggregationResult);
        // TODO: add deserialization
        return (E) result.getResult();
    }

    @Override
    public CollocationMode getCollocationMode() {
        return CollocationMode.EMBEDDED;
    }

    @Override
    protected void createNewMetadataIfRequired() {
        try {
            //TODO: replace LinkedBlockingQueue with more efficient implementation
            List<Object> items = bounded ? new ArrayList<>(capacity) :  new ArrayList<>();
            EmbeddedQueue embeddedQueue = new EmbeddedQueue(queueName, new EmbeddedQueueContainer(items, bounded ? capacity : null));
            space.write(embeddedQueue, WriteModifiers.WRITE_ONLY);
        } catch (EntryAlreadyInSpaceException e) {
            // no-op
        }
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int size() {
        final IdQuery<EmbeddedQueue> idQuery = idQuery().setProjections(QUEUE_CONTAINER_PATH + "." + SIZE_PATH);
        return space.read(idQuery).getContainer().getSize();
    }

    private IdQuery<EmbeddedQueue> idQuery() {
        return new IdQuery<EmbeddedQueue>(EmbeddedQueue.class, queueName);
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

}