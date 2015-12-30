package org.openspaces.collections.serialization;

import java.io.*;
import java.util.Arrays;

/**
 * @author Leonid_Poliakov
 */
public class JavaElementSerializer implements ElementSerializer {

    @Override
    public Object serialize(Object pojo) throws SerializationException {
        if (pojo == null) {
            return null;
        }

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutput objectStream = null;
        try {
            objectStream = new ObjectOutputStream(byteStream);
            objectStream.writeObject(pojo);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Error serializing " + pojo, e);
        } finally {
            try {
                if (objectStream != null) {
                    objectStream.close();
                }
            } catch (IOException ex) {
                // ignore
            }
            try {
                byteStream.close();
            } catch (IOException ex) {
                // ignore
            }
        }
    }

    @Override
    public Object deserialize(Object payload) throws SerializationException {
        if (payload == null) {
            return null;
        }

        byte[] bytes = (byte[]) payload;
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInput objectStream = null;
        try {
            objectStream = new ObjectInputStream(byteStream);
            return objectStream.readObject();
        } catch (Exception e) {
            throw new SerializationException("Error deserializing " + Arrays.toString(bytes), e);
        } finally {
            try {
                byteStream.close();
            } catch (IOException ex) {
                // ignore
            }
            try {
                if (objectStream != null) {
                    objectStream.close();
                }
            } catch (IOException ex) {
                // ignore
            }
        }
    }

}