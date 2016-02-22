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
import org.openspaces.core.ChangeException;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.QUEUE_NAME_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * Blocking queue implementation, that supports embedded collocation mode. 
 * 
 * @author Svitlana_Pogrebna
 */
public class EmbeddedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E, EmbeddedQueueContainer> {

	private final IdQuery<EmbeddedQueueContainer> idQuery;
	private final IdQuery<EmbeddedQueueContainer> idQueryLightWeightContainer;
	
	public static final Integer UNBOUNDED_ALLOCATION = 1000;//TODO:Configurable
    
	private static final Logger LOG = Logger.getLogger(EmbeddedGigaBlockingQueue.class.getName());
	
    /**
     * Creates not bounded queue
     * 
     * @param space      space used to hold queue
     * @param queueName  unique queue name
     * @param serializer element serializer/deserializer
     */
    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, ElementSerializer<E> serializer) {
        super(space, queueName, 0, false, serializer);
        idQuery = createQuereyForToRetrieveQueueMetaData(queueName);
        idQueryLightWeightContainer = createQuereyToRetireveQueueSize(queueName);
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
        idQuery = createQuereyForToRetrieveQueueMetaData(queueName);
        idQueryLightWeightContainer = createQuereyToRetireveQueueSize(queueName);
    }

    private static IdQuery<EmbeddedQueueContainer> createQuereyForToRetrieveQueueMetaData(String queueName){
    	return new IdQuery<EmbeddedQueueContainer>(EmbeddedQueueContainer.class, queueName);
    }
    private static IdQuery<EmbeddedQueueContainer> createQuereyToRetireveQueueSize(String queueName){
    	return new IdQuery<EmbeddedQueueContainer>(EmbeddedQueueContainer.class, queueName).setProjections(SIZE_PATH);
    }
    @Override
    protected EmbeddedSizeChangeListener createSizeChangeListener(EmbeddedQueueContainer queueMetadata) {
        return new EmbeddedSizeChangeListener(queueMetadata);
    }
    
    @Override
    protected boolean writeMultipleEntities(List<Object> c) {
    	 checkNotClosed();
    	 
    	 final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedOfferMultipleOperation(c));

         try{
         	space.change(idQuery(), changeSet);
         }catch(ChangeException ex){
         	//TODO:Do we want to let other exceptions through
         	if(ex.getNumSuccesfullChanges() == 0){
         		LOG.log(Level.SEVERE, "Failed to offer collection", ex);
         		return false;
         	}
         }
         return true;
    }
    @Override
    public boolean writeEntity(E e) {
        checkNotClosed();
        
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedOfferOperation(serialize(e)));

        try{
        	space.change(idQuery(), changeSet);
        }catch(ChangeException ex){
        	//TODO:Do we want to let other exceptions through
        	if(ex.getNumSuccesfullChanges() == 0){
        		LOG.log(Level.SEVERE, "Failed to offer entity" + e.toString(), ex);
        		return false;
        	}
        }
        return true;
       
    }

    @Override
    public boolean contains(Object e) {
        checkNotClosed();

        final AggregationSet aggregationSet = new AggregationSet().add(new EmbeddedContaiinsOperation(serialize((E)e)));
        
       AggregationResult result = space.aggregate(idQuery(), aggregationSet);
       
       Boolean success = toSingleResult(result);
        
        if(success != null){
        	return success;
        }
        
        return false;
       
    }
    @Override
    public boolean remove(Object o){
	    final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedRemoveOperation(serialize((E)o)));
	    
	    try{
	    	space.change(idQuery(), changeSet);
	    }catch(ChangeException ex){
	    	//TODO:Do we want to let other exceptions through
	    	if(ex.getNumSuccesfullChanges() == 0){
	    		LOG.log(Level.SEVERE, "Failed to remove entity" + o.toString(), ex);
	    		return false;
	    	}
	    }
	    return true;
    }
    @Override
    public E poll() {
        checkNotClosed();

        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedPollOperation());

        final ChangeResult<EmbeddedQueueContainer> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);

        return deserialize(toSingleResult(changeResult));
     }

    @Override
    public void clear() {
    	checkNotClosed();

    	final ChangeSet changeSet = new ChangeSet().custom( new EmbeddedClearOperation());
    	
    	space.change(idQuery(), changeSet);
    }

    @Override
    public E peek() {
        checkNotClosed();

        final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(new EmbeddedPeekOperation()));

        return deserialize(toSingleResult(aggregationResult));
        
       
    }

    @Override
    public CollocationMode getCollocationMode() {
        return CollocationMode.EMBEDDED;
    }

    @Override
    protected EmbeddedQueueContainer getOrCreate() {
        try {
            List<Object> items = bounded ? new ArrayList<>(capacity) : new ArrayList<>(UNBOUNDED_ALLOCATION);
            EmbeddedQueueContainer container = new EmbeddedQueueContainer(queueName, items, bounded ? capacity : null);
            space.write(container, WriteModifiers.WRITE_ONLY);
            return container;
        } catch (EntryAlreadyInSpaceException e) {
            return getLightweightContainer();
        }
    }

    @Override
    public Iterator<E> iterator() {
        checkNotClosed();
        return new QueueIterator();
    }

    @Override
    public int size() {
        return getLightweightContainer().getSize();
    }

    private EmbeddedQueueContainer getLightweightContainer() {
        //final IdQuery<EmbeddedQueueContainer> idQuery = idQuery().setProjections(SIZE_PATH);
        return space.read(idQueryLightWeightContainer);
    }
    
    private IdQuery<EmbeddedQueueContainer> idQuery() {
        return idQuery;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        space.clear(idQuery());
    }
   

    private class QueueIterator implements Iterator<E> {

        private static final int DEFAULT_MAX_ENTRIES = 100;

        private final int maxEntries; 
        
        private Object curr;
        private int currIndex;
        
        private int batchIndex;
        private Iterator<Object> iterator;

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
            // TODO:Async?
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

        private Iterator<Object> nextBatch() {
            final EmbeddedRetrieveOperation operation = new EmbeddedRetrieveOperation(batchIndex, maxEntries);
            final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(operation));

            final SerializableResult<List<Object>> result = toSingleResult(aggregationResult);
            final List<Object> items = result.getResult();
            if (items == null) {
                return null;
            }

            batchIndex += items.size();
            return items.iterator();
        }

        //TODO:Would this be accurate when multiple clients updating the same queue?
        //The index may change often?
        @Override
        public void remove() {
            if (curr == null) {
                throw new IllegalStateException();
            }
            
            //TODO:Async?
            final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedRemoveIteratorOperation(currIndex, curr));
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
            SQLQuery<EmbeddedQueueContainer> query = new SQLQuery<>(EmbeddedQueueContainer.class, QUEUE_NAME_PATH + " = ?");// AND " + SIZE_PATH + " <> ?");
            query.setProjections(SIZE_PATH);
            return query;
        }

        @Override
        protected void populateParams(SQLQuery<EmbeddedQueueContainer> query) {
            query.setParameters(queueName, queueMetadata.getSize());
        }
    }
}
