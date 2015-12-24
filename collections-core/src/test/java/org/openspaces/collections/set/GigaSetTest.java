package org.openspaces.collections.set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.CollectionUtils.createSerializableTypeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.AbstractCollectionTest;
import org.springframework.test.context.ContextConfiguration;

@RunWith(Parameterized.class)
@ContextConfiguration(locations = "classpath:/GigaSetTest-context.xml")
public class GigaSetTest extends AbstractCollectionTest<SerializableType> {

    @Resource
    protected GigaSet<SerializableType> gigaSet;
    
    public GigaSetTest(List<SerializableType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList() },
                { Collections.singletonList(createSerializableType()) },
                { createSerializableTypeList(MEDIUM_COLLECTION_SIZE) } });
    }

    @Before
    public void setUp() {
        gigaSet.addAll(testedElements);
    }
    
    @Test
    public void testAdd() {
        final SerializableType newElement = newNotNullElement();
        final Collection<SerializableType> collection = getCollection();
        assertTrue("The result of adding a new element: " + newElement + " should be true", collection.add(newElement));
        int expectedSize = testedElements.size() + 1;
        assertSize("Invalid collection size", expectedSize);
        
        SerializableType existingElement = testedElements.isEmpty() ? newElement : testedElements.iterator().next();
        assertFalse("The result of adding an existing element: " + existingElement + " should be false", collection.add(existingElement));
        assertSize("Invalid collection size", expectedSize);
    }
    
    @Test
    public void testAddAll() {
        final Collection<SerializableType> collection = getCollection();
        assertFalse("Collection should not be changed after adding empty set", collection.addAll(Collections.<SerializableType>emptySet()));
        int expectedSize = testedElements.size();
        assertSize("Invalid collection size", expectedSize);
        
        assertFalse("Collection should not be changed after adding empty list", collection.addAll(Collections.<SerializableType>emptyList()));
        assertSize("Invalid collection size", expectedSize);
        
        assertFalse("Collection should not be changed after adding all its elements", collection.addAll(testedElements));
        assertSize("Invalid collection size", expectedSize);
        
        assertFalse("Collection should not be changed after adding a sub collection of its elements", collection.addAll(getTestedDataSubCollection()));
        assertSize("Invalid collection size", expectedSize);
        
        List<SerializableType> newElementsList = Collections.singletonList(newNotNullElement());
        assertTrue("Collection should be changed after new element from list = " + newElementsList, collection.addAll(newElementsList));
        assertSize("Invalid collection size", ++expectedSize);
        
        Queue<SerializableType> newElementsQueue = new LinkedList<>(newElementsList);
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        newElementsQueue.offer(newNotNullElement());
        expectedSize += 3;
        assertTrue("Collection should be changed after adding new elements from queue = " + newElementsQueue, collection.addAll(newElementsQueue));
        assertSize("Invalid collection size", expectedSize);
        
        Set<SerializableType> newElementsSet = new HashSet<>(testedElements);
        newElementsSet.add(newNotNullElement());
        assertTrue("Collection should be changed after adding existing and new elements from set = " + newElementsSet, collection.addAll(newElementsSet));
        assertSize("Invalid collection size", ++expectedSize);
    }
    
    @Override
    protected SerializableType newNotNullElement() {
        return createSerializableType();
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
    protected Collection<SerializableType> getCollection() {
        return gigaSet;
    }
    
    @Override
    protected void assertSize(String msg, int expectedSize) {
        assertEquals(msg, expectedSize, gigaSpace.count(null));
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAllNull() {
        getCollection().removeAll(null);
    }
}
