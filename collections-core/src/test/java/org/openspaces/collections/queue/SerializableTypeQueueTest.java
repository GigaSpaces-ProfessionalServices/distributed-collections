package org.openspaces.collections.queue;

import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.openspaces.collections.set.SerializableType;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class SerializableTypeQueueTest extends AbstractQueueTest<SerializableType> {

    public SerializableTypeQueueTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList() },
                { Collections.singletonList(createSerializableType()) },
//                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE) },
        });
    }

    @Override
    protected Class<? extends SerializableType> getElementType() {
        return SerializableType.class;
    }

    @Override
    protected SerializableType[] getElementArray() {
        return new SerializableType[testedElements.size()];
    }

    @Override
    protected SerializableType newNotNullElement() {
        return createSerializableType();
    }
}
