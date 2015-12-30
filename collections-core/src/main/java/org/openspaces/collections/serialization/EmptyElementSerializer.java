package org.openspaces.collections.serialization;

/**
 * @author Leonid_Poliakov
 */
public class EmptyElementSerializer implements ElementSerializer {

    @Override
    public Object serialize(Object pojo) throws SerializationException {
        return pojo;
    }

    @Override
    public Object deserialize(Object payload) throws SerializationException {
        return payload;
    }

}