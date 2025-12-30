/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
import oracle.jdbc.OracleConnection;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.RandomUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseTemplate;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-11 20:19
 */
public class ConnectionTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testBatchUpdateContinuesWithTransaction() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            testBatchUpdateWithTransaction();
        }
    }

    @Test
    public void testBatchUpdateWithTransaction() throws InterruptedException {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());

        // 模拟并发
        final int threadSize = 10;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final CyclicBarrier barrier = new CyclicBarrier(threadSize);
        final CountDownLatch latch = new CountDownLatch(threadSize);
        final String sql = "UPDATE `test`.`vote_records` SET `user_id` = ?, `create_time` = now() WHERE `id` = ?";
        final String sql2 = "UPDATE `test`.`vote_records_test` SET `user_id` = ?, `create_time` = now() WHERE `id` = ?";
        for (int i = 0; i < threadSize; i++) {
            final int k = i + 1;
            pool.submit(() -> {
                try {
                    barrier.await();
                    // 模拟操作
                    System.out.println(String.format("%s %s:%s", LocalDateTime.now(), Thread.currentThread().getName(), k));
                    Connection connection = null;
                    try {
                        connection = connectorInstance.getConnection();
                        connection.setAutoCommit(false);
                        DatabaseTemplate template = new DatabaseTemplate((SimpleConnection) connection);
                        Object[] args = new Object[2];
                        args[0] = randomUserId(20);
                        args[1] = k;
                        List<Object[]> list = new ArrayList<>();
                        list.add(args);
                        int[] ints = template.batchUpdate(sql, list);
                        int[] ints2 = template.batchUpdate(sql2, list);
                        connection.commit();
                        System.out.println(String.format("%s %s:执行结果:%s,%s", LocalDateTime.now(), Thread.currentThread().getName(), Arrays.toString(ints), Arrays.toString(ints2)));
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                        throw new SdkException(e.getMessage(), e.getCause());
                    } finally {
                        connectorInstance.getDataSource().close(connection);
                    }
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

    @Test
    public void testByte() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createOracleConfig());

        String executeSql = "UPDATE \"my_user\" SET \"name\"=?,\"clo\"=? WHERE \"id\"=?";
        int[] execute = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.batchUpdate(executeSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) {
                        try {
                            SimpleConnection connection = databaseTemplate.getSimpleConnection();
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
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createSqlServerConfig());

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

                    Object execute = connectorInstance.execute(tem -> tem.queryForObject("select 1", Integer.class));
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

    @Test
    public void testQuery() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());
        // 3、执行SQL
        String querySql = "SELECT * from test_schema where id = ?";
        Object[] args = new Object[1];
        args[0] = 9999999;
        List<Map<String, Object>> list = connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(querySql, args));
        logger.info("test list={}", list);
    }

    @Test
    public void testBatchInsert() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());

        long begin = Instant.now().toEpochMilli();
        final int threadSize = 10;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final String sql = "INSERT INTO `vote_records` (`id`, `user_id`, `vote_num`, `group_id`, `status`, `create_time`) VALUES (?, ?, ?, ?, ?, ?)";

        // 模拟1000w条数据
        List<Object[]> dataList = new ArrayList<>();
        for (int i = 1; i <= 200001; i++) {
            // 442001, 'dA8LeJLtX9MgQgDe7H1O', 9620, 1, 2, '2022-11-17 16:35:21'
            Object[] args = new Object[6];
            args[0] = i;
            args[1] = randomUserId(20);
            args[2] = RandomUtil.nextInt(1, 9999);
            args[3] = RandomUtil.nextInt(0, 3);
            args[4] = RandomUtil.nextInt(1, 3);
            args[5] = Timestamp.valueOf(LocalDateTime.now());
            dataList.add(args);

            if (i % 10000 == 0) {
                System.out.println(i + "-----------------正在处理");
                batchUpdate(connectorInstance, pool, sql, dataList, 1000);
                dataList.clear();
            }
        }

        if (!CollectionUtils.isEmpty(dataList)) {
            System.out.println("-----------------正在处理剩余数据");
            batchUpdate(connectorInstance, pool, sql, dataList, 1000);
        }

        pool.shutdown();
        logger.info("总共耗时：{}秒", (Instant.now().toEpochMilli() - begin) / 1000);
    }

    @Test
    public void testBatchUpdate() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());

        long begin = Instant.now().toEpochMilli();
        final int threadSize = 10;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final String sql = "UPDATE `test`.`vote_records` SET `user_id` = ?, `create_time` = now() WHERE `id` = ?";

        // 模拟100w条数据
        int k = 10;
        while (k > 0) {
            List<Object[]> dataList = new ArrayList<>();
            for (int i = 1; i <= 100000; i++) {
                // 'dA8LeJLtX9MgQgDe7H1O', '2022-11-17 16:35:21', 1
                Object[] args = new Object[2];
                args[0] = randomUserId(20);
                args[1] = i;
                dataList.add(args);

                if (i % 10000 == 0) {
                    System.out.println(i + "-----------------正在处理");
                    batchUpdate(connectorInstance, pool, sql, dataList, 1000);
                    dataList.clear();
                }
            }

            if (!CollectionUtils.isEmpty(dataList)) {
                System.out.println("-----------------正在处理剩余数据");
                batchUpdate(connectorInstance, pool, sql, dataList, 1000);
            }
            k--;
        }

        pool.shutdown();
        logger.info("总共耗时：{}秒", (Instant.now().toEpochMilli() - begin) / 1000);
    }

    @Test
    public void testBatchDelete() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());

        long begin = Instant.now().toEpochMilli();
        final int threadSize = 10;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final String sql = "delete from `test`.`vote_records` WHERE `id` = ?";

        List<Object[]> dataList = new ArrayList<>();
        for (int i = 1; i <= 3259000; i++) {
            // 'dA8LeJLtX9MgQgDe7H1O', '2022-11-17 16:35:21', 1
            Object[] args = new Object[1];
            args[0] = i;
            dataList.add(args);

            if (i % 10000 == 0) {
                System.out.println(i + "-----------------正在处理");
                batchUpdate(connectorInstance, pool, sql, dataList, 1000);
                dataList.clear();
            }
        }

        if (!CollectionUtils.isEmpty(dataList)) {
            System.out.println("-----------------正在处理剩余数据");
            batchUpdate(connectorInstance, pool, sql, dataList, 1000);
        }

        pool.shutdown();
        logger.info("总共耗时：{}秒", (Instant.now().toEpochMilli() - begin) / 1000);
    }

    @Test
    public void testBatchIUD() {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());

        long begin = Instant.now().toEpochMilli();
        final int threadSize = 1000;
        final int num = 300;
        final ExecutorService pool = Executors.newFixedThreadPool(threadSize);
        final CountDownLatch latch = new CountDownLatch(threadSize);
        final String insert = "INSERT INTO `vote_records_test` (`id`, `user_id`, `vote_num`, `group_id`, `status`, `create_time`) VALUES (?, ?, ?, ?, ?, ?)";
        final String update = "UPDATE `test`.`vote_records_test` SET `user_id` = ?, `create_time` = now() WHERE `id` = ?";
        final String delete = "DELETE from `test`.`vote_records_test` WHERE `id` = ?";

        // 模拟单表增删改事件
        for (int i = 0; i < threadSize; i++) {
            final int offset = i;
            pool.submit(() -> {
                try {
                    logger.info("{}-开始任务", Thread.currentThread().getName());
                    // 增删改事件密集型
                    mockData(connectorInstance, num, offset, insert, update, delete);
                    logger.info("{}-结束任务", Thread.currentThread().getName());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        pool.shutdown();
        logger.info("总数：{}, 耗时：{}秒", (threadSize * num), (Instant.now().toEpochMilli() - begin) / 1000);
    }

    private void mockData(DatabaseConnectorInstance connectorInstance, int num, int offset, String insert, String update, String delete) {
        int start = offset * num;
        logger.info("{}-offset:{}, start:{}", Thread.currentThread().getName(), offset, start);
        List<Object[]> insertData = new ArrayList<>();
        List<Object[]> updateData = new ArrayList<>();
        List<Object[]> deleteData = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            // insert
            Object[] insertArgs = new Object[6];
            insertArgs[0] = i + start;
            insertArgs[1] = randomUserId(20);
            insertArgs[2] = RandomUtil.nextInt(1, 9999);
            insertArgs[3] = RandomUtil.nextInt(0, 3);
            insertArgs[4] = RandomUtil.nextInt(1, 3);
            insertArgs[5] = Timestamp.valueOf(LocalDateTime.now());
            insertData.add(insertArgs);

            // update
            Object[] updateArgs = new Object[2];
            updateArgs[0] = randomUserId(20);
            updateArgs[1] = i + start;
            updateData.add(updateArgs);

            // delete
            Object[] deleteArgs = new Object[1];
            deleteArgs[0] = i + start;
            deleteData.add(deleteArgs);

            connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(insert, insertData));
            connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(update, updateData));
            connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(delete, deleteData));
            insertData.clear();
            updateData.clear();
            deleteData.clear();
            logger.info("{}, 数据行[{}, {}], 已处理:{}", Thread.currentThread().getName(), start, start + num, i + start);
        }
    }

    private final static String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private String randomUserId(int i) {
        StringBuilder s = new StringBuilder();
        for (int j = 0; j < i; j++) {
            int r = RandomUtil.nextInt(0, 62);
            s.append(StringUtil.substring(STR, r, r + 1));
        }
        return s.toString();
    }

    private void batchUpdate(DatabaseConnectorInstance connectorInstance, ExecutorService pool, String sql, List<Object[]> dataList, int batchSize) {
        int total = dataList.size();
        int taskSize = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;
        final CountDownLatch latch = new CountDownLatch(taskSize);
        int offset = 0;
        for (int i = 0; i < taskSize; i++) {
            List<Object[]> slice = dataList.stream().skip(offset).limit(batchSize).collect(Collectors.toList());
            offset += batchSize;
            pool.submit(() -> {
                try {
                    connectorInstance.execute(databaseTemplate -> databaseTemplate.batchUpdate(sql, slice));
                } catch (Exception e) {
                    logger.error(e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void testReadSchema() {
        List<Table> tables = getTables(createOracleConfig(), "test", "AE86", "MY_ORG");
        assert !tables.isEmpty();
        List<Table> tables1 = getTables(createOracleConfig(), "test", "AE86", null);
        assert !tables1.isEmpty();

        getTables(createMysqlConfig(), "test", "root", "MY_ORG");
        getTables(createMysqlConfig(), "test", "root", null);

        getTables(createSqlServerConfig(), "test", "dbo", "MY_ORG");
        getTables(createSqlServerConfig(), "test", "dbo", null);

        getTables(createPostgresConfig(), "postgres", "public", "MY_ORG");
        getTables(createPostgresConfig(), "postgres", "public", null);
    }

    @Test
    public void testGetColumnsDetails() {
        final String schema = "root";
        final String tableNamePattern = "sw_test";
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(createMysqlConfig());
        connectorInstance.execute(databaseTemplate -> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            String databaseCatalog = conn.getCatalog();
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            List<Field> fields = new ArrayList<>();
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columnMetadata = metaData.getColumns(databaseCatalog, schemaNamePattern, tableNamePattern, null);
            while (columnMetadata.next()) {
                String columnName = columnMetadata.getString(4);
                int columnType = columnMetadata.getInt(5);
                String typeName = columnMetadata.getString(6);
                fields.add(new Field(columnName, typeName, columnType));
            }
            return fields;
        });
    }

    private List<Table> getTables(DatabaseConfig config, final String catalog, final String schema, final String tableNamePattern) {
        final DatabaseConnectorInstance connectorInstance = new DatabaseConnectorInstance(config);
        List<Table> tables = new ArrayList<>();
        connectorInstance.execute(databaseTemplate -> {
            SimpleConnection connection = databaseTemplate.getSimpleConnection();
            Connection conn = connection.getConnection();
            String databaseCatalog = null == catalog ? conn.getCatalog() : catalog;
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            String[] types = {TableTypeEnum.TABLE.getCode(), TableTypeEnum.VIEW.getCode(), TableTypeEnum.MATERIALIZED_VIEW.getCode()};
            final ResultSet rs = conn.getMetaData().getTables(databaseCatalog, schemaNamePattern, tableNamePattern, types);
            while (rs.next()) {
                final String tableName = rs.getString("TABLE_NAME");
                final String tableType = rs.getString("TABLE_TYPE");
                Table table = new Table();
                table.setName(tableName);
                table.setType(tableType);
                tables.add(table);
            }
            return tables;
        });

        logger.info("\r 表总数{}", tables.size());
        tables.forEach(t -> logger.info("{} {}", t.getName(), t.getType()));

        return tables;
    }

    private DatabaseConfig createSqlServerConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test");
        config.setUsername("sa");
        config.setPassword("123");
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return config;
    }

    private DatabaseConfig createOracleConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:ORCL");
        config.setUsername("ae86");
        config.setPassword("123");
        config.setDriverClassName("oracle.jdbc.OracleDriver");
        return config;
    }

    private DatabaseConfig createMysqlConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:mysql://127.0.0.1:3305/test?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false");
        config.setUsername("root");
        config.setPassword("123");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return config;
    }

    private DatabaseConfig createPostgresConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        config.setUsername("postgres");
        config.setPassword("123456");
        config.setDriverClassName("org.postgresql.Driver");
        return config;
    }
}