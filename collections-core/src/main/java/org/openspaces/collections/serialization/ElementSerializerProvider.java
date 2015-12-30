package org.openspaces.collections.serialization;

/**
 * @author Leonid_Poliakov
 */
public interface ElementSerializerProvider {

    /**
     * Returns one of serializers appropriate for given class.
     *
     * @param clazz the type of objects to be serialized, may be <code>null</code>
     * @return the serializer implementation
     */
    ElementSerializer pickSerializer(Class clazz);

}