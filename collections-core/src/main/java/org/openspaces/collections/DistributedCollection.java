package org.openspaces.collections;

import java.util.Collection;

/**
 * The interface to be implemented by distributed collections
 *
 * @param <T> the type of elements in this collection
 * @author Svitlana_Pogrebna
 */
public interface DistributedCollection<T> extends Collection<T>, AutoCloseable {

    /**
     * @return unique collection name
     */
    String getName();

    /**
     * @return collocation mode
     */
    CollocationMode getCollocationMode();

    /**
     * Deletes the collection from the grid
     */
    void destroy() throws Exception;
}
