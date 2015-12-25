package org.openspaces.collections.queue;

import java.util.concurrent.BlockingQueue;

import org.openspaces.collections.DistributedCollection;

/**
 * Distributed Blocking Queue
 *
 * @author Oleksiy_Dyagilev
 */
public interface GigaBlockingQueue<E> extends BlockingQueue<E>, DistributedCollection<E> {
}
