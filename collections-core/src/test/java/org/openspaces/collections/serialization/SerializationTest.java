package org.openspaces.collections.serialization;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import org.openspaces.collections.set.NonSerializableType;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Objects;

import static org.openspaces.collections.CollectionUtils.createNonSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.testng.Assert.assertEquals;

/**
 * @author Leonid_Poliakov
 */
@ContextConfiguration
public class SerializationTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private GigaSpace gigaSpace;

    @BeforeMethod
    public void setUp() {
        gigaSpace.clear(new Object());
    }

    @Test
    public void testKryoSerializer() {
        testSerializer(new KryoElementSerializer<SerializableType>(), createSerializableType());
        testSerializer(new KryoElementSerializer<NonSerializableType>(), createNonSerializableType());
        testSerializer(new KryoElementSerializer<>(), null);
    }

    @Test
    public void testJavaSerializer() {
        testSerializer(new JavaElementSerializer<SerializableType>(), createSerializableType());
        testSerializer(new JavaElementSerializer<>(), null);
    }

    @Test
    public void testDefaultProvider() {
        DefaultSerializerProvider provider = new DefaultSerializerProvider();

        SerializableType serializable = createSerializableType();
        testSerializer(provider.pickSerializer(serializable.getClass()), serializable);

        NonSerializableType nonSerializable = createNonSerializableType();
        testSerializer(provider.pickSerializer(nonSerializable.getClass()), nonSerializable);

        testSerializer(provider.pickSerializer(null), null);
        testSerializer(provider.pickSerializer(null), serializable);
        testSerializer(provider.pickSerializer(null), nonSerializable);
    }
    
    private <T> void testSerializer(ElementSerializer<T> serializer, T object) {
        byte[] payload = serializer.serialize(object);
        Object answer = serializer.deserialize(payload);
        assertEquals(answer, object);

        SpaceWrapper wrapper = new SpaceWrapper(serializer.serialize(object));
        gigaSpace.write(wrapper);

        SpaceWrapper template = new SpaceWrapper();
        template.setId(wrapper.getId());
        SpaceWrapper spaceAnswer = gigaSpace.read(template);
        assertEquals(serializer.deserialize(spaceAnswer.getObject()), object);
    }

    @SpaceClass
    public static class SpaceWrapper {
        private String id;
        private byte[] object;

        public SpaceWrapper() {
        }

        public SpaceWrapper(byte[] object) {
            this.object = object;
        }

        @SpaceId(autoGenerate = true)
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public byte[] getObject() {
            return object;
        }

        public void setObject(byte[] object) {
            this.object = object;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + Arrays.hashCode(object);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SpaceWrapper other = (SpaceWrapper) obj;
            return Objects.equals(id, other.id) ? Arrays.equals(object, other.object) : false;
        }
    }

}