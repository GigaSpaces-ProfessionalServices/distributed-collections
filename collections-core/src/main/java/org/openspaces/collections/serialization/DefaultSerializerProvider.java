package org.openspaces.collections.serialization;

import java.io.Serializable;

/**
 * Provides one of two serializers: java-based for Serializable objects and Kryo for others.
 *
 * @author Leonid_Poliakov
 */
public class DefaultSerializerProvider implements ElementSerializerProvider {

    @SuppressWarnings("unchecked")
    public <T> ElementSerializer<T> pickSerializer(Class<? extends T> clazz) {
        if (clazz == null) {
            return new KryoElementSerializer<>();
        }
        if (Serializable.class.isAssignableFrom(clazz)) {
            return (ElementSerializer<T>) new JavaElementSerializer<Serializable>();
        }
        return new KryoElementSerializer<>();
    }
}