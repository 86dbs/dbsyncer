import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    public void testByte() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:XE");
        config.setUsername("ae86");
        config.setPassword("123");
        config.setDriverClassName("oracle.jdbc.OracleDriver");
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(config);

        String executeSql="UPDATE \"my_user\" SET \"name\"=?,\"clo\"=? WHERE \"id\"=?";
        int[] execute = connectorMapper.execute(databaseTemplate ->
                databaseTemplate.batchUpdate(executeSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) {
                        try {
                            SimpleConnection connection = (SimpleConnection) databaseTemplate.getConnection();
                            OracleConnection conn = (OracleConnection) connection.getConnection();
                            Clob clob = conn.createClob();
                            clob.setString(1, new String("中文888".getBytes(Charset.defaultCharset())));

                            ps.setString(1, "hello888");
                            ps.setClob(2, clob);
                            ps.setInt(3, 2);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return 1;
                    }
                })
        );
        logger.info("execute:{}", execute);
    }

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