package org.openspaces.collections.set;

import net.jini.core.transaction.Transaction;
import org.openspaces.core.map.LockHandle;
import org.openspaces.core.transaction.TransactionProvider;

import java.io.Serializable;
import java.util.Set;

public interface GigaSet<T extends Serializable> extends Set<T> {

    /**
     * Returns the transaction provider allowing accessing the current running transaction.
     */
    TransactionProvider getTxProvider();

    /**
     * Returns the current running transaction. Can be <code>null</code> if no transaction is in progress.
     */
    Transaction getCurrentTransaction();

    /**
     * @param e          element to be added to this set
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     */
    boolean add(T e, long timeToLive);

    /**
     * @param e          element to be added to this set
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @param timeout    A timeout to use if the object is locked under a transaction (in milliseconds)
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     */
    boolean add(T e, long timeToLive, long timeout);

    /**
     * @param e          element to be added to this set
     * @param lockHandle If the element is locked, will perform the operation within the same lock
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     */
    boolean add(T e, LockHandle lockHandle);

    /**
     * @param e          element to be added to this set
     * @param timeToLive time to keep object in this cache, in milliseconds
     * @param lockHandle If the element is locked, will perform the operation within the same lock
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     */
    boolean add(T e, long timeToLive, LockHandle lockHandle);

    /**
     * @param element         element to be removed from this set
     * @param waitForResponse time to wait for response
     * @return The removed object
     */
    boolean remove(Object e, long waitForResponse);

    /**
     * @param element         element to be removed from this set
     * @param waitForResponse time to wait for response
     * @param lockHandle      If the element is locked, will perform the operation within the same lock
     * @return The removed object
     */
    boolean remove(Object e, long waitForResponse, LockHandle lockHandle);

    /**
     * Locks the given element for any updates. Returns a {@link org.openspaces.core.map.LockHandle}
     * that can be bused to perform specific updates under the same lock (by calling
     * {@link #put(Object, Object, org.openspaces.core.map.LockHandle)} for example).
     * <p/>
     * <p>Will use the configured default lock time to live and default waiting for lock timeout
     * values. By default the lock time to live is 60 seconds and waiting for lock timeout is
     * 10 seconds.
     *
     * @param e The element The element to lock
     * @return LockHandle that can be used to perform operations under the given lock
     */
    LockHandle lock(T e);

    /**
     * Locks the given element for any updates. Returns a {@link org.openspaces.core.map.LockHandle}
     * that can be used to perform specific updates under the same lock (by using the transaction
     * object stored within it).
     *
     * @param element               The element to lock
     * @param lockTimeToLive        The lock time to live (in milliseconds)
     * @param waitingForLockTimeout The time to wait for an already locked lock
     * @return LockHandle that can be used to perform operations under the given lock
     */
    LockHandle lock(T e, long lockTimeToLive, long waitingForLockTimeout);

    /**
     * Unlocks the given lock on the element
     *
     * @param element The element to unlock
     */
    void unlock(T e);

    /**
     * Returns <code>true</code> if the given element is locked. Otherwise returns <code>false</code>.
     *
     * @param e element to check if it locked or not.
     * @return <code>true</code> if the given element is locked or not.
     */
    boolean isLocked(T e);

    /**
     * Unlocks the given element and adds the given element in a single operation.
     *
     * @param e element to be added to this set
     */
    void addAndUnlock(T e);

    /**
     * Returns the default time to live of entries in the map.
     */
    long getDefaultTimeToLive();

    /**
     * Returns the default wait for response value for entries in the map.
     */
    long getDefaultWaitForResponse();

}
