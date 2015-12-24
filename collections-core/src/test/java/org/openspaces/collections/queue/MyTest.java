package org.openspaces.collections.queue;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.set.SerializableType;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Iterator;

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
		DefaultGigaBlockingQueue<SerializableType> queue = new DefaultGigaBlockingQueue<>(gigaSpace, "test-queue", 100);
//
//        Runnable runnable = new Runnable() {
//            @Override
//            public void run() {
//                queue.offer(new SerializableType());
//            }
//        };
////
//
//        try {
//            Thread.sleep(55000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        System.out.println(queue.offer(new SerializableType(1L)));
        System.out.println(queue.offer(new SerializableType(2L)));
        System.out.println(queue.offer(new SerializableType(3L)));
//        System.out.println("queue.size() = " + queue.size());

        Iterator<SerializableType> iterator = queue.iterator();
//
//
        iterator.next();
        iterator.remove();
//        iterator.next();
//        iterator.remove();
//        iterator.next();
//        iterator.remove();

//        while (iterator.hasNext()) {
//            SerializableType next = iterator.next();
//            System.out.println("next = " + next);
//        }

        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());

        //        System.out.println("111");
    }

}
