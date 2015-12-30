package org.openspaces.collections;

import java.util.Collection;

/**
 * The interface to be implemented by distributed collections
 *
 * @param <T> the type of elements in this collection
 * @author Svitlana_Pogrebna
 */
public interface DistributedCollection<T> extends Collection<T>, AutoCloseable {

    String getName();

    CollocationMode getCollocationMode();
}
