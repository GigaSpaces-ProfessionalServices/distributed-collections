package org.openspaces.collections.queue.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.io.Serializable;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;

/**
 * @author Oleksiy_Dyagilev
 */
public class SizeOperation extends CustomChangeOperation {

    @Override
    public Object change(MutableServerEntry entry) {
        Long tail = (Long) entry.getPathValue(TAIL_PATH);
        Long head = (Long) entry.getPathValue(HEAD_PATH);

        long size = tail - head;

        return new Result((int) size);
    }

    @Override
    public String getName() {
        return "size";
    }

    /**
     * Operation result
     */
    public static class Result implements Serializable {
        private int size;

        public Result() {
        }

        public Result(int size) {
            this.size = size;
        }

        public int getSize() {
            return size;
        }
    }
}
