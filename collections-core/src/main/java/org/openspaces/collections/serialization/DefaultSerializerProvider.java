package org.openspaces.collections.serialization;

import java.io.Serializable;

/**
 * Provides one of two serializers: java-based for Serializable objects and Kryo for others.
 *
 * @author Leonid_Poliakov
 */
public class DefaultSerializerProvider implements ElementSerializerProvider {

    public ElementSerializer pickSerializer(Class clazz) {
        if (clazz == null) {
            return new KryoElementSerializer();
        }
        if (Serializable.class.isAssignableFrom(clazz)) {
            return new JavaElementSerializer();
        }
        return new KryoElementSerializer();
    }

}