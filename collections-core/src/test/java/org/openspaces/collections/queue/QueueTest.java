package org.openspaces.collections.queue;

import com.j_spaces.core.client.SQLQuery;
import org.openspaces.collections.BasicCollectionTest;
import org.openspaces.collections.CollectionUtils;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.GigaQueueConfigurer;
import org.openspaces.collections.queue.distributed.data.DistrQueueItem;
import org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer;
import org.openspaces.collections.set.NonSerializableType;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openspaces.collections.util.TestUtils.combination;
import static org.testng.Assert.*;

@ContextConfiguration("classpath:/partitioned-space-test-config.xml")
public class QueueTest extends BasicCollectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(QueueTest.class);

    @DataProvider
    public static Object[][] queueTypes() {
        Object[][] types = combination(
                /* collocation      */ Arrays.asList(CollocationMode.DISTRIBUTED, CollocationMode.LOCAL),
                /* bounded queue    */ Arrays.asList(false, true),
                /* serializable/not */ Arrays.asList(true, false)
        );
        LOG.info("Testing {} queue combinations", types.length);
        return types;
    }

    @Factory(dataProvider = "queueTypes")
    public static Object[] createTests(CollocationMode collocation, boolean bounded, boolean serializable) {
        return new Object[]{new QueueTest(collocation, bounded, serializable)};
    }

    private static final long TIMEOUT = 1000;
    private static final long TIMEOUT_ACCURACY = 10;
    private static int capacity = 100;
    private static int elementCount = 10;

    @Autowired
    private GigaSpace gigaSpace;

    private CollocationMode collocation;
    private boolean bounded;
    private boolean serializable;
    private String queueName;
    private GigaBlockingQueue<Object> queue;
    private List<Object> content;

    public QueueTest(CollocationMode collocation, boolean bounded, boolean serializable) {
        this.collocation = collocation;
        this.bounded = bounded;
        this.serializable = serializable;
    }

    @BeforeClass(groups = "all")
    public void setUp() {
        LOG.info("Setting up queue: collocation = {}, bounded = {}, serializable = {}", collocation, bounded, serializable);
        queueName = "test-queue-" + collocation.name().toLowerCase() + "-" + (bounded ? "bounded" : "unbounded") + "-" + (serializable ? "serializable" : "nonserializable");
        Class<?> elementType = serializable ? SerializableType.class : NonSerializableType.class;
        queue = new GigaQueueConfigurer(gigaSpace, queueName, collocation)
                .capacity(bounded ? capacity : null)
                .elementType(elementType)
                .gigaQueue();
    }

    @BeforeClass(groups = "filled")
    public void logFilled() {
        LOG.info("Queue will be filled with {} elements", elementCount);
    }

    @BeforeClass(groups = "empty")
    public void logEmpty() {
        LOG.info("Queue will be empty");
    }

    @BeforeMethod(groups = "filled")
    public void fillQueue(Method method) {
        content = CollectionUtils.createSerializableTypeList(elementCount);
        queue.clear();
        queue.addAll(content);
        LOG.info("| running {}", method.getName());
    }

    @BeforeMethod(groups = "empty")
    public void emptyQueue(Method method) {
        content = Collections.emptyList();
        queue.clear();
        LOG.info("| running {}", method.getName());
    }

    @AfterClass(groups = "all")
    public void tearDown() throws Exception {
        LOG.info("Closing queue: collocation = {}, bounded = {}", collocation, bounded);
        queue.close();

        // just to separate logs
        LOG.info("...");
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testOfferNull() {
        queue.offer(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testOfferNullWithTimeout() throws InterruptedException {
        queue.offer(null, 1, SECONDS);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testPutNull() throws InterruptedException {
        queue.put(null);
    }

    @Test(groups = "empty", expectedExceptions = NoSuchElementException.class)
    public void testElementEmpty() {
        queue.element();
    }

    @Test(groups = "filled")
    public void testElement() {
        Object head = queue.element();
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size());

        Object sameHead = queue.element();
        assertEquals(sameHead, head, "Blocking queue head should be the same");
    }

    @Test(groups = "filled")
    public void testPeek() {
        Object head = queue.peek();
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size());

        Object sameHead = queue.peek();
        assertEquals(sameHead, head, "Blocking queue head should be the same");
    }

    @Test(groups = "empty")
    public void testPeekEmpty() {
        assertNull(queue.peek(), "The retrieved element should be null");
    }

    @Test(groups = "filled")
    public void testRemove() {
        Object head = queue.remove();
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size() - 1);
    }

    @Test(groups = "empty", expectedExceptions = NoSuchElementException.class)
    public void testRemoveEmpty() {
        queue.remove();
    }

    @Test(groups = "filled")
    public void testPoll() {
        Object head = queue.poll();
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size() - 1);

        if (content.size() > 1) {
            Object sameHead = queue.poll();
            assertNotEquals(head, sameHead, "Blocking queue head should not be the same");
            assertEquals(head, content.get(0), "Invalid blocking queue head");
            assertSize(content.size() - 2);
        }
    }

    @Test(groups = "filled")
    public void testPollWithTimeout() throws InterruptedException {
        Object head = queue.poll(TIMEOUT, MILLISECONDS);
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size() - 1);

        if (content.size() > 1) {
            Object sameHead = queue.poll(TIMEOUT, MILLISECONDS);
            assertNotEquals(head, sameHead, "Blocking queue head should not be the same");
            assertEquals(head, content.get(0), "Invalid blocking queue head");
            assertSize(content.size() - 2);
        }
    }

    @Test(groups = "empty")
    public void testPollEmpty() {
        assertNull(queue.poll(), "The retrieved element should be null");
    }

    @Test(groups = "empty", timeOut = TIMEOUT)
    public void testPollWithTimeoutEmpty() throws InterruptedException {
        assertNull(queue.poll(TIMEOUT - TIMEOUT_ACCURACY, MILLISECONDS));
    }

    @Test(groups = "empty")
    public void testPollAndMeasureWithTimeoutEmpty() throws InterruptedException {
        long start = System.currentTimeMillis();
        assertNull(queue.poll(TIMEOUT + TIMEOUT_ACCURACY, MILLISECONDS));
        long end = System.currentTimeMillis();

        assertTrue(end - start >= TIMEOUT + TIMEOUT_ACCURACY);
    }

    @Test(groups = "all")
    public void testAdd() {
        Object element = newElement();
        assertTrue(queue.add(element), "The element should be added");
        assertSize(content.size() + 1);
        if (content.isEmpty()) {
            assertEquals(queue.peek(), element, "Invalid blocking queue head");
        }

        assertTrue(queue.add(element), "The element should be added");
        assertSize(content.size() + 2);
    }

    @Test(groups = "all")
    public void testAddAll() {
        assertFalse(queue.addAll(Collections.emptySet()), "Queue should not be changed");
        assertSize(content.size());

        assertTrue(queue.addAll(newElements(elementCount)), "Blocking queue should be changed");
        assertSize(content.size() + elementCount);
    }

    @Test(groups = "all")
    public void testOffer() {
        Object element = newElement();
        assertTrue(queue.offer(element), "The element should be added");
        assertSize(content.size() + 1);
        if (content.isEmpty()) {
            assertEquals(queue.peek(), element, "Invalid blocking queue head");
        }

        assertTrue(queue.offer(element), "The element should be added");
        assertSize(content.size() + 2);
    }

    @Test(groups = "all")
    public void testOfferWithTimeout() throws InterruptedException {
        Object element = newElement();
        assertTrue(queue.offer(element, TIMEOUT, MILLISECONDS), "The element should be added");
        assertSize(content.size() + 1);
        if (content.isEmpty()) {
            assertEquals(queue.peek(), element, "Invalid blocking queue head");
        }

        assertTrue(queue.offer(element, TIMEOUT, MILLISECONDS), "The element should be added");
        assertSize(content.size() + 2);
    }

    @Test(groups = "all")
    public void testPut() throws InterruptedException {
        Object element = newElement();
        queue.put(element);
        assertSize(content.size() + 1);
        if (content.isEmpty()) {
            assertEquals(queue.peek(), element, "Invalid blocking queue head");
        }

        queue.put(element);
        assertSize(content.size() + 2);
    }

    @Test(groups = "filled", timeOut = 5000)
    public void testTake() throws InterruptedException {
        Object head = queue.take();
        assertEquals(head, content.get(0), "Invalid blocking queue head");
        assertSize(content.size() - 1);
    }

    @Test(groups = "all")
    public void testRemainingCapacity() {
        if (bounded) {
            assertCapacity(capacity - content.size());
            Object element = newElement();
            assertTrue(queue.add(element));
            assertCapacity(capacity - content.size() - 1);

            assertTrue(queue.remove(element));
            assertCapacity(capacity - content.size());

            queue.clear();
            assertCapacity(capacity);

            List<Object> elements = newElements(elementCount);
            assertTrue(queue.addAll(elements));
            assertCapacity(capacity - elementCount);

            assertTrue(queue.removeAll(elements));
            assertCapacity(capacity);
        } else {
            assertCapacity(Integer.MAX_VALUE);
            Object element = newElement();
            assertTrue(queue.add(element));
            assertCapacity(Integer.MAX_VALUE);

            assertTrue(queue.remove(element));
            assertCapacity(Integer.MAX_VALUE);

            queue.clear();
            assertCapacity(Integer.MAX_VALUE);

            List<Object> elements = newElements(elementCount);
            assertTrue(queue.addAll(elements));
            assertCapacity(Integer.MAX_VALUE);

            assertTrue(queue.removeAll(elements));
            assertCapacity(Integer.MAX_VALUE);
        }
    }

    @Test(groups = "all")
    public void testDrainTo() {
        Collection<Object> result = new ArrayList<>();
        assertEquals(queue.drainTo(result), content.size(), "Invalid number of elements transferred after performing 'drainTo' operation");

        assertSize(0, "Blocking queue should be empty");
        assertEquals(result.size(), content.size(), "Invalid result collection size");
        for (Object actual : result) {
            assertNotNull(actual, "Element should not be null");
            assertTrue(content.contains(actual), "Invalid element");
        }
    }

    @Test(groups = "all")
    public void testDrainToMaxElements() {
        Collection<Object> result = new ArrayList<>();
        assertEquals(0, queue.drainTo(result, Integer.MIN_VALUE), "No elements should be transferred due to negative max elements param");
        assertSize(content.size(), "Blocking queue size should not be changed");

        if (content.isEmpty()) {
            assertEquals(queue.drainTo(result, 1), 0, "Invalid number of elements transferred after performing 'drainTo' operation");
            assertSize(0, "Blocking queue should be empty");
            assertEquals(result.size(), 0, "Invalid result collection size");
        } else {
            assertEquals(queue.drainTo(result, 1), 1, "Invalid number of elements transferred after performing 'drainTo' operation");
            assertSize(content.size() - 1, "Blocking queue size should not be changed");
            assertEquals(result.size(), 1, "Invalid result collection size");

            Object element = result.iterator().next();
            assertNotNull(element, "Element should not be null");
            // relies on the elements order preserving
            assertEquals(element, content.iterator().next(), "Invalid element transferred");
        }

        if (content.size() > 1) {
            assertEquals(queue.drainTo(result, Integer.MAX_VALUE), content.size() - 1, "Invalid number of elements transferred after performing 'drainTo' operation");
            assertSize(0, "Blocking queue should be empty");
            assertEquals(result.size(), content.size(), "Invalid result collection size");
            for (Object actual : result) {
                assertNotNull(actual, "Element should not be null");
                assertTrue(content.contains(actual), "Invalid element");
            }
        }
    }

    @Test(groups = "all", expectedExceptions = IllegalArgumentException.class)
    public void testDrainToSameCollection() {
        queue.drainTo(queue);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testDrainToNullCollection() {
        queue.drainTo(null);
    }

    @Test(groups = "all", expectedExceptions = NullPointerException.class)
    public void testDrainToMaxElementsNullCollection() {
        queue.drainTo(null, Integer.MAX_VALUE);
    }

    @Test(groups = "all", expectedExceptions = IllegalArgumentException.class)
    public void testDrainToMaxElementsSameCollection() {
        queue.drainTo(queue, Integer.MAX_VALUE);
    }

    @Test(groups = "filled")
    public void testCallingRemoveOnIteratorConcurrently() {
        // creating iterator before the polling
        Iterator<Object> iterator = queue.iterator();

        // polling half of the elements
        for (int index = 0; index < content.size() / 2; index++) {
            queue.poll();
        }
        int expectedSize = content.size() - content.size() / 2;

        // removing first item from iterator - that should be polled in loop above
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.remove();

        // checking queue size - should not be affected by iterator
        assertEquals(expectedSize, queue.size());
    }

    @Test(groups = "all")
    public void testCallingRemoveOnLastElement() {
        Object element = newElement();
        queue.add(element);
        // calls iterator.remove() inside
        queue.remove(element);

        assertEquals(content.size(), queue.size());

        queue.clear();
        assertEquals(0, queue.size());
    }

    @Test(groups = "all")
    public void testRemovingWholeQueueWithIterator() {
        Iterator<Object> iterator = queue.iterator();
        while (iterator.hasNext()) {
            assertNotNull(iterator.next());
            iterator.remove();
        }
        assertSize(0, "Queue must be empty");
    }

    @Test(groups = "filled")
    public void testRemovingMiddleElementWithIterator() {
        int middleIndex = content.size() / 2 - 1;
        Iterator<Object> iterator = queue.iterator();
        for (int index = 0; index <= middleIndex; index++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }
        iterator.remove();

        assertSize(content.size() - 1, "Queue must be missing one element");

        for (int index = 0; index < content.size(); index++) {
            // skip middle element
            if (index != middleIndex) {
                Object expected = content.get(index);
                assertEquals(queue.poll(), expected);
            }
        }
    }

    @Test(groups = "all", expectedExceptions = IllegalStateException.class)
    public void testIteratorRemoveWithoutNext() {
        queue.iterator().remove();
    }

    @Test(groups = "filled", expectedExceptions = IllegalStateException.class)
    public void testIteratorRemoveTwiceWithoutNext() {
        Iterator<Object> iterator = queue.iterator();
        iterator.next();
        iterator.remove();
        iterator.remove();
    }

    @Test(groups = "all")
    public void testIteratorRemoveDuplicatedItems() {
        Object item1 = newElement();
        queue.add(item1);
        Object item2 = newElement();
        queue.add(item2);
        queue.add(item1);

        Iterator<Object> iterator = queue.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        iterator.remove();

        assertSize(content.size() + 2);
        for (Object object : content) {
            assertEquals(queue.poll(), object);
        }
        assertEquals(queue.poll(), item1);
        assertEquals(queue.poll(), item2);
    }

    @Test(groups = "all", expectedExceptions = {IllegalStateException.class, SkipException.class})
    public void testAddToFullQueue() {
        fillUpBoundedQueue();
        queue.add(newElement());
    }

    @Test(groups = "all", expectedExceptions = {IllegalStateException.class, SkipException.class})
    public void testAddAllToFullQueue() {
        fillUpBoundedQueue();
        queue.addAll(newElements(elementCount));
    }

    @Test(groups = "all")
    public void testOfferToFullQueue() {
        fillUpBoundedQueue();
        assertFalse(queue.offer(newElement()), "The element should not be inserted due to reaching the capacity");
    }

    @Test(groups = "all")
    public void testOfferWithTimeoutFullQueue() throws InterruptedException {
        fillUpBoundedQueue();
        assertFalse(queue.offer(newElement(), TIMEOUT, MILLISECONDS), "The element should not be inserted due to reaching the capacity");
    }

    @Test(groups = "all", timeOut = TIMEOUT)
    public void testPutIntoFullQueueAndRemoveWithIterator() throws InterruptedException {
        fillUpBoundedQueue();
        final Object element = newElement();
        Thread puttingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queue.put(element);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        puttingThread.start();

        int middleIndex = capacity / 2;
        Iterator<Object> iterator = queue.iterator();
        for (int index = 0; index <= middleIndex; index++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }
        iterator.remove();

        puttingThread.join();

        int size = queue.size();
        assertEquals(size, capacity);
        for (int index = 0; index < size - 1; index++) {
            assertNotNull(queue.poll());
        }
        assertEquals(queue.poll(), element);
    }

    @Override
    protected List<Object> getContent() {
        return content;
    }

    @Override
    protected Collection<Object> getCollection() {
        return queue;
    }

    @Override
    protected Object newElement() {
        return serializable ? CollectionUtils.createSerializableType() : CollectionUtils.createNonSerializableType();
    }

    @Override
    protected Class<?> getElementType() {
        return SerializableType.class;
    }

    @Override
    protected Object[] getElementArray() {
        return content.toArray();
    }

    @Override
    protected void assertSize(int expected, String message) {
        switch (collocation) {
            case EMBEDDED:
                SQLQuery<EmbeddedQueueContainer> embeddedQuery = new SQLQuery<>(EmbeddedQueueContainer.class, "name = ?", queueName);
                int actual = gigaSpace.read(embeddedQuery).getItems().size();
                assertEquals(actual, expected, message);
                break;
            case LOCAL:
                // fall through
            case DISTRIBUTED:
                SQLQuery<DistrQueueItem> distributedQuery = new SQLQuery<>(DistrQueueItem.class, "itemKey.queueName = ?", queueName);
                assertEquals(gigaSpace.count(distributedQuery), expected, message);
                break;
        }
    }

    private void assertCapacity(int expected) {
        assertEquals(queue.remainingCapacity(), expected, "Invalid remaining capacity");
    }

    private void fillUpBoundedQueue() {
        if (!bounded) {
            throw new SkipException("Testing only bounded queues");
        }
        queue.addAll(newElements(capacity - content.size()));
    }

}