package org.openspaces.core.collections;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import net.jini.core.transaction.Transaction;

import org.openspaces.core.GigaMap;
import org.openspaces.core.map.LockHandle;
import org.openspaces.core.transaction.TransactionProvider;
public class DefaultGigaSet<T extends Serializable> extends AbstractSet<T> implements GigaSet<T> {

    private static final String NULL_ELEMENT_ERR_MSG = "'GigaSet' does not permit null elements";
    
    private static final Serializable DUMMY = new DummyValue();
    
    private GigaMap gigaMap;
    
    public DefaultGigaSet(GigaMap gigaMap) {
        this.gigaMap = requireNonNull(gigaMap, "'gigaMap' parameter must not be null");
    }

    @Override
    public int size() {
        return gigaMap.size();
    }

    @Override
    public boolean isEmpty() {
        return gigaMap.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        final Object element = requireNonNull(o, NULL_ELEMENT_ERR_MSG);
        return gigaMap.containsKey(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<T> iterator() {
        return gigaMap.keySet().iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean add(T e) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return gigaMap.put(element, DUMMY) == null;
    }
    
    @Override
    public boolean add(T e, long timeToLive) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return gigaMap.put(element, DUMMY, timeToLive) == null;
    }

    @Override
    public boolean add(T e, long timeToLive, long timeout) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return gigaMap.put(element, DUMMY, timeToLive, timeout) == null;
    }

    @Override
    public boolean add(T e, LockHandle lockHandle) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return gigaMap.put(element, DUMMY, lockHandle) == null;
    }

    @Override
    public boolean add(T e, long timeToLive, LockHandle lockHandle) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return gigaMap.put(element, DUMMY, timeToLive, lockHandle) == null;
    }

    @Override
    public boolean remove(Object e) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return DUMMY.equals(gigaMap.remove(element));
    }

    @Override
    public boolean remove(Object e, long waitForResponse) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return DUMMY.equals(gigaMap.remove(element, waitForResponse));
    }

    @Override
    public boolean remove(Object e, long waitForResponse, LockHandle lockHandle) {
        final Object element = requireNonNull(e, NULL_ELEMENT_ERR_MSG);
        return DUMMY.equals(gigaMap.remove(element, waitForResponse, lockHandle));
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (Object e : c) {
            modified |= remove(e);
        }
        return modified;
    }
     
    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<T> it = iterator();
        while (it.hasNext()) {
            T element = it.next();
            if (!c.contains(element)) {
                modified |= remove(element);
            }
        }
        return modified;
    }
    
    @Override
    public void clear() {
        gigaMap.clear();
    }

    @Override
    public TransactionProvider getTxProvider() {
        return gigaMap.getTxProvider();
    }

    @Override
    public Transaction getCurrentTransaction() {
        return gigaMap.getCurrentTransaction();
    }

    @Override
    public LockHandle lock(T e) {
        return gigaMap.lock(e);
    }

    @Override
    public LockHandle lock(T e, long lockTimeToLive, long waitingForLockTimeout) {
       return gigaMap.lock(e, lockTimeToLive, waitingForLockTimeout);
    }

    @Override
    public void unlock(T e) {
       gigaMap.unlock(e);
    }

    @Override
    public boolean isLocked(T e) {
        return gigaMap.isLocked(e);
    }

    @Override
    public void addAndUnlock(T e) {
        gigaMap.putAndUnlock(e, DUMMY);
    }

    @Override
    public long getDefaultTimeToLive() {
        return gigaMap.getDefaultTimeToLive();
    }

    @Override
    public long getDefaultWaitForResponse() {
        return gigaMap.getDefaultWaitForResponse();
    }
    
    private static class DummyValue implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean equals(Object o) {
            return o == null ? false : true;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }
}
