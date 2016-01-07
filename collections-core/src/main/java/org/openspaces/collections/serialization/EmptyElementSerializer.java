package org.openspaces.collections.serialization;

/**
 * @author Leonid_Poliakov
 */
public class EmptyElementSerializer<T> implements ElementSerializer<T> {

    @Override
    public Object serialize(T pojo) throws SerializationException {
        return pojo;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(Object payload) throws SerializationException {
        return (T)payload;
    }

}