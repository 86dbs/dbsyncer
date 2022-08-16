import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.enums.TableTypeEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.nio.charset.Charset;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/11 20:19
 */
public class ConnectionTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testByte() {
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(createOracleConfig());

        String executeSql = "UPDATE \"my_user\" SET \"name\"=?,\"clo\"=? WHERE \"id\"=?";
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
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(createSqlServerConfig());

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

    @Test
    public void testReadSchema() {
        getTables(createOracleConfig(), "test", "AE86", "MY_ORG");
        getTables(createOracleConfig(), "test", "AE86", null);

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
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(createMysqlConfig());
        connectorMapper.execute(databaseTemplate -> {
            SimpleConnection connection = (SimpleConnection) databaseTemplate.getConnection();
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
        final DatabaseConnectorMapper connectorMapper = new DatabaseConnectorMapper(config);
        List<Table> tables = new ArrayList<>();
        connectorMapper.execute(databaseTemplate -> {
            SimpleConnection connection = (SimpleConnection) databaseTemplate.getConnection();
            Connection conn = connection.getConnection();
            String databaseCatalog = null == catalog ? conn.getCatalog() : catalog;
            String schemaNamePattern = null == schema ? conn.getSchema() : schema;
            String[] types = {TableTypeEnum.TABLE.getCode(), TableTypeEnum.VIEW.getCode(), TableTypeEnum.MATERIALIZED_VIEW.getCode()};
            final ResultSet rs = conn.getMetaData().getTables(databaseCatalog, schemaNamePattern, tableNamePattern, types);
            while (rs.next()) {
                final String tableName = rs.getString("TABLE_NAME");
                final String tableType = rs.getString("TABLE_TYPE");
                tables.add(new Table(tableName, tableType));
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
        config.setUrl("jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useSSL=false&verifyServerCertificate=false&autoReconnect=true&failOverReadOnly=false");
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