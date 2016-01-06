package org.openspaces.collections.serialization;

import java.io.*;
import java.util.Arrays;

/**
 * @author Leonid_Poliakov
 */
public class JavaElementSerializer<T extends Serializable> implements ElementSerializer<T> {

    @Override
    public byte[] serialize(T pojo) throws SerializationException {
        if (pojo == null) {
            return null;
        }
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                ObjectOutput objectStream = new ObjectOutputStream(byteStream)) {

            objectStream.writeObject(pojo);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Error serializing " + pojo, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(byte[] payload) throws SerializationException {
        if (payload == null) {
            return null;
        }
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(payload);
                ObjectInput objectStream = new ObjectInputStream(byteStream)) {
            
            return (T) objectStream.readObject();
        } catch (Exception e) {
            throw new SerializationException("Error deserializing " + Arrays.toString(payload), e);
        }
    }
}