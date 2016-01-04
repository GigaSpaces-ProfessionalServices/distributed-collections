package org.openspaces.collections.queue.concurrent;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Oleksiy_Dyagilev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/gigaqueue-distributed-test-context.xml")
public class DistributedQueueProducerConsumerTest extends BaseProducerConsumerTest {
}
