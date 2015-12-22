package org.openspaces.collections.queue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.set.ComplexType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * TODO: Temp testl
 *
 * @author Oleksiy_Dyagilev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
public class MyTest {

    @Autowired
    protected GigaSpace gigaSpace;

    @Test
    public void test() {

        DefaultGigaBlockingQueue<ComplexType> queue = new DefaultGigaBlockingQueue<>(gigaSpace, "test-queue", 1);
        System.out.println(queue.offer(new ComplexType()));
        System.out.println("queue.size() = " + queue.size());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println("queue.size() = " + queue.size());

        System.out.println(queue.offer(new ComplexType()));
        System.out.println("queue.size() = " + queue.size());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println("queue.size() = " + queue.size());


//        System.out.println("111");
    }

}
