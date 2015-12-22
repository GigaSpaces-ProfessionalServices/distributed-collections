package org.openspaces.collections.set;

import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createComplexType;
import static org.openspaces.collections.CollectionUtils.createComplexTypeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class ComplexTypeGigaSetTest extends AbstractGigaSetTest<ComplexType> {

    public ComplexTypeGigaSetTest(List<ComplexType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { { Collections.emptyList() },
                { Collections.singletonList(createComplexType()) },
                { createComplexTypeList(MEDIUM_COLLECTION_SIZE) } });
    }

    @Override
    protected ComplexType newNotNullElement() {
        return createComplexType();
    }
    
    @Override
    protected Class<? extends ComplexType> getElementType() {
        return ComplexType.class;
    }

    @Override
    protected ComplexType[] getElementArray() {
        return new ComplexType[testedElements.size()];
    }
}
