import oracle.jdbc.dcn.TableChangeDescription;
import org.apache.commons.lang.math.RandomUtils;
import org.dbsyncer.listener.oracle.event.DCNEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LinkedBlockingQueueTest {

    private final Logger                  logger = LoggerFactory.getLogger(getClass());
    private       BlockingQueue queue  = new LinkedBlockingQueue<>(10);

    @Test
    public void testProducerAndConsumer() throws InterruptedException {
        logger.info("test begin");
        new Producer(queue).start();
        new Consumer(queue).start();
        new Consumer(queue).start();
        TimeUnit.SECONDS.sleep(60);
        logger.info("test end");
    }

    /**
     * 生产
     */
    class Producer extends Thread {

        BlockingQueue<DCNEvent> queue;
        int taskNumber = 50;

        public Producer(BlockingQueue<DCNEvent> queue) {
            setName("Producer-thread");
            this.queue = queue;
        }

        @Override
        public void run() {
            logger.info("生产线程{}开始工作", Thread.currentThread().getName());
            for (int i = 0; i < taskNumber; i++) {
                DCNEvent event = new DCNEvent("my_user" + i, "AAAF8BAABAAALJBAAA", TableChangeDescription.TableOperation.INSERT.getCode());
                try {
                    // 如果BlockQueue没有空间,则调用此方法的线程被阻断直到BlockingQueue里面有空间再继续
                    queue.put(event);
                } catch (InterruptedException e) {
                    logger.error("添加消息：{}, 失败", event, e.getMessage());
                }
            }
            logger.info("生产线程{}结束工作", Thread.currentThread().getName());
        }
    }

    /**
     * 消费
     */
    class Consumer extends Thread {

        BlockingQueue<DCNEvent> queue;

        public Consumer(BlockingQueue<DCNEvent> queue) {
            setName("Consumer-thread-" + RandomUtils.nextInt(100));
            this.queue = queue;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            logger.info("消费线程{}开始工作", threadName);
            while (true) {
                try {
                    // 模拟耗时
                    TimeUnit.SECONDS.sleep(RandomUtils.nextInt(3));
                    // 取走BlockingQueue里排在首位的对象,若BlockingQueue为空,阻断进入等待状态直到Blocking有新的对象被加入为止
                    DCNEvent event = queue.take();
                    logger.error("消费线程{}接受消息：{}", threadName, event.getTableName());
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

}