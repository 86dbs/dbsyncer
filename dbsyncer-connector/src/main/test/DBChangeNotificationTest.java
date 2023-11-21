import oracle.jdbc.OracleStatement;
import oracle.jdbc.driver.OracleConnection;
import org.dbsyncer.connector.oracle.dcn.DBChangeNotification;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-05-10 22:25
 */
public class DBChangeNotificationTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testConnect() throws Exception {
        String username = "ae86";
        String password = "123";
        String url = "jdbc:oracle:thin:@127.0.0.1:1521:XE";

        final DBChangeNotification dcn = new DBChangeNotification(username, password, url);
        dcn.addRowEventListener((e) ->
            logger.info("{}触发{}, data:{}", e.getSourceTableName(), e.getEvent(), e.getDataList())
        );
        dcn.start();

        // 模拟并发
        final int threadSize = 301;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final CyclicBarrier barrier = new CyclicBarrier(threadSize);
        final CountDownLatch latch = new CountDownLatch(threadSize);

        for (int i = 0; i < threadSize; i++) {
            final int k = i + 3;
            pool.submit(() -> {
                try {
                    barrier.await();
                    //read(k, dcn);

                    // 模拟写入操作
                    insert(k, dcn);

                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                } catch (BrokenBarrierException e) {
                    logger.error(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            logger.info("try to close");
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        pool.shutdown();

        TimeUnit.SECONDS.sleep(20);
        dcn.close();
        logger.info("test end");

    }

    private void insert(int k, DBChangeNotification dcn) {
        OracleConnection conn = dcn.getOracleConnection();
        OracleStatement os = null;
        ResultSet rs = null;
        try {
            os = (OracleStatement) conn.createStatement();
            String sql = "INSERT INTO \"AE86\".\"my_user\"(\"id\", \"name\", \"age\", \"phone\", \"create_date\", \"last_time\", \"money\", \"car\", \"big\", \"clo\", \"rel\") VALUES (" + k + ", '红包', '2', '18200001111', TO_DATE('2015-10-23 00:00:00', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2021-01-23 00:00:00.000000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '200.00000000000000', '4', null, '888', '3.0000000000000000')";

            int i = os.executeUpdate(sql);
            logger.info("insert:{}, {}", k, i);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            dcn.close(rs);
            dcn.close(os);
        }
    }

    private void read(final int k, DBChangeNotification dcn) {
        final String tableName = "my_user";
        final String rowId = "AAAE5fAABAAALCJAAx";
        List<Object> data = new ArrayList<>();
        dcn.read(tableName, rowId, data);
        logger.info("{}, 【{}】, data:{}", k, data.size(), data);
    }

}