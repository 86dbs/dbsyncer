import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/11 20:19
 */
public class SqlServerConnectionTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testConnection() throws InterruptedException {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test");
        config.setUsername("sa");
        config.setPassword("123");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(config);

        // 模拟并发
        final int threadSize = 100;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final CyclicBarrier barrier = new CyclicBarrier(threadSize);
        final CountDownLatch latch = new CountDownLatch(threadSize);
        for (int i = 0; i < threadSize; i++) {
            final int k = i + 3;
            pool.submit(() -> {
                try {
                    barrier.await();

                    // 模拟操作
                    System.out.println(String.format("%s %s:%s", LocalDateTime.now(), Thread.currentThread().getName(), k));

                    Object execute = connectorMapper.execute(tem -> tem.queryForObject("select 1", Integer.class));
                    System.out.println(String.format("%s %s:%s execute=>%s", LocalDateTime.now(), Thread.currentThread().getName(), k, execute));

                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                } catch (BrokenBarrierException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            logger.info("try to shutdown");
            pool.shutdown();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        TimeUnit.SECONDS.sleep(3);
        logger.info("test end");
    }
}
