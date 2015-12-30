package org.openspaces.collections.queue;

import org.openspaces.collections.DistributedCollection;

import java.util.concurrent.BlockingQueue;

/**
 * Distributed Blocking Queue
 *
 * @author Oleksiy_Dyagilev
 */
public interface GigaBlockingQueue<E> extends BlockingQueue<E>, DistributedCollection<E> {
}
