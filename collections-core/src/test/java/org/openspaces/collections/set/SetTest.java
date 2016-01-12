package org.openspaces.collections.set;

import org.openspaces.collections.BasicCollectionTest;
import org.openspaces.collections.CollectionUtils;
import org.openspaces.collections.GigaSetConfigurer;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.*;

import static org.openspaces.collections.CollectionUtils.createSerializableType;
import static org.openspaces.collections.util.TestUtils.combination;
import static org.testng.Assert.*;

@ContextConfiguration
public class SetTest extends BasicCollectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(SetTest.class);

    @DataProvider
    public static Object[][] setTypes() {
        Object[][] types = combination(
                /* clustered/not/undefined */ Arrays.asList(true, false, null)
        );
        LOG.info("Testing {} set combinations", types.length);
        return types;
    }

    @Factory(dataProvider = "setTypes")
    public static Object[] createTests(Boolean clustered) {
        return new Object[]{new SetTest(clustered)};
    }

    private static int elementCount = 10;

    @Autowired
    @Qualifier("gigaSpace")
    private GigaSpace gigaSpace;
    @Autowired
    @Qualifier("embeddedGigaSpace")
    private GigaSpace embeddedGigaSpace;

    private Boolean clustered;
    private GigaSet<SerializableType> set;
    private List<SerializableType> content;

    public SetTest(Boolean clustered) {
        this.clustered = clustered;
    }

    @BeforeClass(groups = "all")
    public void setUp() {
        LOG.info("Setting up set: clustered = {}", clustered);
        GigaSpace targetSpace = gigaSpace;
        if (clustered != null && !clustered) {
            targetSpace = embeddedGigaSpace;
        }
        set = new GigaSetConfigurer<SerializableType>(targetSpace).clustered(clustered).gigaSet();
    }

    @BeforeClass(groups = "filled")
    public void logFilled() {
        LOG.info("Set will be filled with {} elements", elementCount);
    }

    @BeforeClass(groups = "empty")
    public void logEmpty() {
        LOG.info("Set will be empty");
    }

    @BeforeMethod(groups = "filled")
    public void fillSet(Method method) {
        content = CollectionUtils.createSerializableTypeList(elementCount);
        set.clear();
        set.addAll(content);
        LOG.info("| running {}", method.getName());
    }

    @BeforeMethod(groups = "empty")
    public void emptySet(Method method) {
        content = Collections.emptyList();
        set.clear();
        LOG.info("| running {}", method.getName());
    }

    @AfterClass(groups = "all")
    public void tearDown() throws Exception {
        // just to separate logs
        LOG.info("...");
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testAddNull() {
        getCollection().add(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testAddAllNullElement() {
        List<SerializableType> objects = Collections.singletonList(null);
        getCollection().addAll(objects);
    }

    @Test(groups = "all")
    public void testAdd() {
        final SerializableType newElement = newElement();
        assertTrue(set.add(newElement), "The result of adding a new element: " + newElement + " should be true");
        int expectedSize = content.size() + 1;
        assertSize(expectedSize);

        SerializableType existingElement = content.isEmpty() ? newElement : content.iterator().next();
        assertFalse(set.add(existingElement), "The result of adding an existing element: " + existingElement + " should be false");
        assertSize(expectedSize);
    }

    @Test(groups = "all")
    public void testAddAll() {
        assertFalse(set.addAll(Collections.<SerializableType>emptySet()), "Collection should not be changed after adding empty set");
        int expectedSize = content.size();
        assertSize(expectedSize);

        assertFalse(set.addAll(Collections.<SerializableType>emptyList()), "Collection should not be changed after adding empty list");
        assertSize(expectedSize);

        assertFalse(set.addAll(content), "Collection should not be changed after adding all its elements");
        assertSize(expectedSize);

        assertFalse(set.addAll((Collection) getContentSubCollection()), "Collection should not be changed after adding a sub collection of its elements");
        assertSize(expectedSize);

        List<SerializableType> newElementsList = Collections.singletonList(newElement());
        assertTrue(set.addAll(newElementsList), "Collection should be changed after new element from list = " + newElementsList);
        assertSize(++expectedSize);

        Queue<SerializableType> newElementsQueue = new LinkedList<>(newElementsList);
        newElementsQueue.offer(newElement());
        newElementsQueue.offer(newElement());
        newElementsQueue.offer(newElement());
        expectedSize += 3;
        assertTrue(set.addAll(newElementsQueue), "Collection should be changed after adding new elements from queue = " + newElementsQueue);
        assertSize(expectedSize);

        Set<SerializableType> newElementsSet = new HashSet<>(content);
        newElementsSet.add(newElement());
        assertTrue(set.addAll(newElementsSet), "Collection should be changed after adding existing and new elements from set = " + newElementsSet);
        assertSize(++expectedSize);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRemoveNull() {
        getCollection().remove(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testContainsNull() {
        getCollection().contains(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRemoveAllNullElement() {
        getCollection().removeAll(Arrays.asList(null));
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testContainsAllNullElement() {
        getCollection().containsAll(Arrays.asList(null));
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testRetainAllNullElement() {
        getCollection().retainAll(Arrays.asList(null));
    }

    @Override
    protected SerializableType newElement() {
        return createSerializableType();
    }

    @Override
    protected Class<? extends SerializableType> getElementType() {
        return SerializableType.class;
    }

    @Override
    protected Object[] getElementArray() {
        return content.toArray();
    }

    @Override
    protected List<Object> getContent() {
        return (List) content;
    }

    @Override
    protected Collection<Object> getCollection() {
        return (Collection) set;
    }

    @Override
    protected void assertSize(int expectedSize, String msg) {
        assertEquals(getCollection().size(), expectedSize, msg);
    }

}
