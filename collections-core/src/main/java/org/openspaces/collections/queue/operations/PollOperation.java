package org.openspaces.collections.queue.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.io.Serializable;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;

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
    public static class Result implements Serializable {
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
    }
}
