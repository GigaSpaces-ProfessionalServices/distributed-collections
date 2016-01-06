package org.openspaces.collections.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;

import java.io.ByteArrayOutputStream;

/**
 * @author Leonid_Poliakov
 */
public class KryoElementSerializer<T> implements ElementSerializer<T> {
    private KryoFactory kryoFactory;

    /**
     * Kryo is not thread safe
     */
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            return kryoFactory.create();
        }
    };

    /**
     * Constructs serializer with given factory
     */
    public KryoElementSerializer(KryoFactory kryoFactory) {
        this.kryoFactory = kryoFactory;
    }

    /**
     * Constructs serializer with default settings
     */
    public KryoElementSerializer() {
        this.kryoFactory = new KryoFactory() {
            public Kryo create() {
                return new Kryo();
            }
        };
    }

    @Override
    public byte[] serialize(T pojo) throws SerializationException {
        if (pojo == null) {
            return null;
        }

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        Output output = new Output(byteStream);
        kryos.get().writeClassAndObject(output, pojo);
        output.flush();
        output.close();
        return byteStream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(byte[] payload) throws SerializationException {
        if (payload == null) {
            return null;
        }

        Input input = new Input(payload);
        Object result = kryos.get().readClassAndObject(input);
        input.close();
        return (T)result;
    }
}