package org.openspaces.collections.serialization;

/**
 * @author Leonid_Poliakov
 */
public interface ElementSerializer<T> {

    /**
     * Serializes actual POJO object into payload object.
     *
     * @param pojo an object to be serialized
     * @return serialized form of the object
     */
    Object serialize(T pojo) throws SerializationException;

    /**
     * Deserializes payload into an actual object.
     *
     * @param payload serialized form of object
     * @return the deserialized object
     */
    T deserialize(Object payload) throws SerializationException;

}