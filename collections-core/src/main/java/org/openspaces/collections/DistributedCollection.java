package org.openspaces.collections;

import java.util.Collection;

/**
 * The interface to be implemented by distributed collections
 * 
 * @author Svitlana_Pogrebna
 *
 * @param <T> the type of elements in this collection
 */
public interface DistributedCollection<T> extends Collection<T>, AutoCloseable {

    String getName();
    
    CollocationMode getCollocationMode();
}
