package org.openspaces.collections.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class SerializationUtils {

    private SerializationUtils() {
    }
    
    public static void writeNullableObject(ObjectOutput out, Object o) throws IOException {
        out.writeBoolean(o == null);
        if (o != null) {
            out.writeObject(o);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T readNullableObject(ObjectInput in) throws ClassNotFoundException, IOException {
        return in.readBoolean() ? null : (T)in.readObject(); 
    }
}
