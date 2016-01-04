/**
 *
 */
package org.openspaces.collections.queue.embedded.operations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedQueueChangeResult<T> implements Externalizable {

    private T result;

    public EmbeddedQueueChangeResult() {
    }

    public EmbeddedQueueChangeResult(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeNullableObject(out, getResult());
    }


    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setResult((T) readNullableObject(in));
    }
}
