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
@Ignore
public class SerializableTypeGigaBlockingQueueTest extends AbstractGigaBlockingQueueTest<SerializableType> {

    public SerializableTypeGigaBlockingQueueTest(List<SerializableType> elements, int capacity) {
        super(elements, capacity);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList(), 1 },
                { Collections.singletonList(createSerializableType()), 2 },
                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE), 2 * MEDIUM_COLLECTION_SIZE },
                /* { createComplexTypeList(LARGE_COLLECTION_SIZE) } */
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
