/**
 * 
 */
package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedQueueItemResult implements Externalizable {

    private Object item; 
    
    public EmbeddedQueueItemResult() {
    }
    
    public EmbeddedQueueItemResult(Object item) {
        this.item = item;
    }
    
    public Object getItem() {
        return item;
    }

    public void setItem(Object item) {
        this.item = item;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeNullableObject(out, getItem());
    }

   
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setItem(readNullableObject(in));
    }
}
