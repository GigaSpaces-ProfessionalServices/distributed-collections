package org.openspaces.collections.queue;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * TODO: Temp testl
 *
 * @author Oleksiy_Dyagilev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class MyTest {

    @Autowired
    protected GigaSpace gigaSpace;

    @After
    public void tearDown() {
        gigaSpace.clear(null);
    }
    
    @Test
    public void test() {
		DefaultGigaBlockingQueue<SerializableType> queue = new DefaultGigaBlockingQueue<>(gigaSpace, "test-queue", 1);
        System.out.println(queue.offer(new SerializableType()));
        System.out.println("queue.size() = " + queue.size());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println("queue.size() = " + queue.size());

        System.out.println(queue.offer(new SerializableType()));
        System.out.println("queue.size() = " + queue.size());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println("queue.size() = " + queue.size());
        
        //        System.out.println("111");
    }

}
