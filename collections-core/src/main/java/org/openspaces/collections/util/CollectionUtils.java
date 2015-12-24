package org.openspaces.collections.util;

public final class CollectionUtils {

    private CollectionUtils() {
    }
    
    public static boolean isQueueEmpty(Long head, Long tail) {
        return head.equals(tail);
    }
    
    public static void checkIndexValid(long index, Long head, Long tail, boolean bounded, int capacity) {
        if (index >= head && index <= tail && (!bounded && index <= capacity)) {
            throw new IndexOutOfBoundsException("Queue item index = " + index + " is out bounds: head = " + head + " tail = " + tail);
        }
    }
}
