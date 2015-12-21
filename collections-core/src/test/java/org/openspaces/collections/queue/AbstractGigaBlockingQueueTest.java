package org.openspaces.collections.queue;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.AbstractCollectionTest;

@RunWith(Parameterized.class)
public abstract class AbstractGigaBlockingQueueTest<T> extends AbstractCollectionTest<T> {

    protected GigaBlockingQueue<T> gigaQueue;
    
    private static final int DEFAULT_CAPACITY = 10;
    
    protected int capacity;
    
    public AbstractGigaBlockingQueueTest(Collection<T> elements) {
        super(elements);
        this.capacity = elements.size() + DEFAULT_CAPACITY;
    }

    @Override
    protected Collection<T> getCollection() {
        return gigaQueue;
    }
}
