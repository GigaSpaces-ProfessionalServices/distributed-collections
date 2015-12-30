package org.openspaces.collections.queue;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.collections.CollocationMode;
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
        ElementSerializer serializer = new DefaultSerializerProvider().pickSerializer(SerializableType.class);
        DistributedGigaBlockingQueue<SerializableType> queue = new DistributedGigaBlockingQueue<>(gigaSpace, "test-queue", 100, CollocationMode.DISTRIBUTED, serializer);
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

//        System.out.println(queue.offer(new SerializableType(2L)));
//        System.out.println(queue.offer(new SerializableType(3L)));
//
        System.out.println("queue.size() = " + queue.size());

        System.out.println(queue.poll());

//        Iterator<SerializableType> iterator = queue.iterator();
//
//
//        iterator.next();
//        iterator.remove();
//        iterator.next();
//        iterator.remove();
//        iterator.next();
//        iterator.remove();

//        while(iterator.hasNext()) {
//            iterator.next();
//            iterator.remove();
//        }

//        while (iterator.hasNext()) {
//            SerializableType next = iterator.next();
//            System.out.println("next = " + next);
//        }

//        System.out.println("queue.size() = " + queue.size());
//
//        System.out.println(queue.offer(new SerializableType(2L)));
//        System.out.println(queue.offer(new SerializableType(3L)));
//
//        System.out.println("queue.size() = " + queue.size());


//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());

        //        System.out.println("111");
    }

}
