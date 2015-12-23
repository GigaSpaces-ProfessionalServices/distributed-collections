package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;
import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;
/**
 * @author Oleksiy_Dyagilev
 */
public class PollOperation extends CustomChangeOperation {

    @Override
    public Object change(MutableServerEntry entry) {
        Long tail = (Long) entry.getPathValue(TAIL_PATH);
        Long head = (Long) entry.getPathValue(HEAD_PATH);
        if (isEmpty(tail, head)) {
            return Result.emptyQueueResult();
        } else {
            long newHead = head + 1;
            entry.setPathValue(HEAD_PATH, newHead);
            return Result.polledIndexResult(newHead);
        }
    }

    private boolean isEmpty(Long tail, Long head) {
        return head.equals(tail);
    }

    @Override
    public String getName() {
        return "poll";
    }

    /**
     * Operation result
     */
    public static class Result implements Externalizable {
        private boolean queueEmpty;
        private Long polledIndex;

        public Result() {
        }

        public static Result emptyQueueResult() {
            Result result = new Result();
            result.queueEmpty = true;
            return result;
        }

        public static Result polledIndexResult(Long polledIndex) {
            Result result = new Result();
            result.queueEmpty = false;
            result.polledIndex = polledIndex;
            return result;
        }

        public boolean isQueueEmpty() {
            return queueEmpty;
        }

        public Long getPolledIndex() {
            return polledIndex;
        }
        
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(isQueueEmpty());
            writeNullableObject(out, getPolledIndex());
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.queueEmpty = in.readBoolean();
            this.polledIndex = readNullableObject(in);
        }
    }
}
