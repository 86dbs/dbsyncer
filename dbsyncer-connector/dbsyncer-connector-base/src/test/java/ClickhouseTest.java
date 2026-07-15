/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.clickhouse.ClickHouseConnector;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.connector.FullPluginContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse 批量写入 / 覆盖写入 / 修改 / 删除 验证
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-13 00:51
 */
public class ClickhouseTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String DATABASE = "xhtb";
    private static final String TABLE_NAME = "dbsyncer_ch_writer_test";
    private static final int DATA_SIZE = 10000;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private ClickHouseConnector connector;
    private DatabaseConnectorInstance connectorInstance;
    private List<Field> fields;
    private Table table;

    @Before
    public void setUp() {
        connector = new ClickHouseConnector();
        DatabaseConfig config = createClickHouseConfig();
        DefaultConnectorServiceContext serviceContext = new DefaultConnectorServiceContext();
        serviceContext.setCatalog(DATABASE);
        serviceContext.setSchema(DATABASE);
        connectorInstance = (DatabaseConnectorInstance) connector.connect(config, serviceContext);

        fields = buildFields();
        table = new Table();
        table.setName(TABLE_NAME);
        table.setType(TableTypeEnum.TABLE.getCode());
        table.setColumn(fields);

        createTestTable();
        truncateTable();
    }

    @After
    public void tearDown() {
        if (connectorInstance != null) {
            connectorInstance.close();
        }
    }

    @Test
    public void testBatchInsert() {
        long start = System.currentTimeMillis();
        List<Map> rows = buildRows(1, DATA_SIZE, "insert");
        Result result = write(ConnectorConstant.OPERTION_INSERT, rows, false);

        Assert.assertTrue(result.getError().length() == 0 ? null : result.getError().toString(), result.getFailData().isEmpty());
        Assert.assertEquals(DATA_SIZE, result.getSuccessData().size());
        Assert.assertEquals(Long.valueOf(DATA_SIZE), countRows());
        Assert.assertEquals("insert_1", queryNameById(1L));
        Assert.assertEquals("insert_" + DATA_SIZE, queryNameById(DATA_SIZE));
        logger.info("批量写入成功, success={}, count={}, cost={}ms",
                result.getSuccessData().size(), countRows(), System.currentTimeMillis() - start);
    }

    @Test
    public void testForceUpdateOverwrite() {
        long start = System.currentTimeMillis();
        Result insertResult = write(ConnectorConstant.OPERTION_INSERT, buildRows(1, DATA_SIZE, "old"), false);
        Assert.assertTrue(insertResult.getFailData().isEmpty());
        Assert.assertEquals(Long.valueOf(DATA_SIZE), countRows());

        List<Map> overwriteRows = buildRows(1, DATA_SIZE, "new");
        for (Map row : overwriteRows) {
            row.put("age", ((Number) row.get("age")).intValue() + 100);
        }
        Result overwriteResult = write(ConnectorConstant.OPERTION_INSERT, overwriteRows, true);
        Assert.assertTrue(overwriteResult.getError().length() == 0 ? null : overwriteResult.getError().toString(),
                overwriteResult.getFailData().isEmpty());
        Assert.assertEquals(DATA_SIZE, overwriteResult.getSuccessData().size());

        // MergeTree 同主键不应产生重复行
        Assert.assertEquals(Long.valueOf(DATA_SIZE), countRows());
        Assert.assertEquals("new_1", queryNameById(1L));
        Assert.assertEquals(Integer.valueOf(101), queryAgeById(1L));
        Assert.assertEquals("new_" + DATA_SIZE, queryNameById(DATA_SIZE));
        Assert.assertEquals(Integer.valueOf(DATA_SIZE + 100), queryAgeById(DATA_SIZE));
        logger.info("覆盖写入成功, count={}, cost={}ms", countRows(), System.currentTimeMillis() - start);
    }

    @Test
    public void testBatchUpdate() {
        long start = System.currentTimeMillis();
        Result insertResult = write(ConnectorConstant.OPERTION_INSERT, buildRows(1, DATA_SIZE, "before"), false);
        Assert.assertTrue(insertResult.getFailData().isEmpty());

        List<Map> updateRows = buildRows(1, DATA_SIZE, "after");
        for (Map row : updateRows) {
            row.put("remark", "updated");
            row.put("age", 88);
        }
        Result updateResult = write(ConnectorConstant.OPERTION_UPDATE, updateRows, false);
        Assert.assertTrue(updateResult.getError().length() == 0 ? null : updateResult.getError().toString(),
                updateResult.getFailData().isEmpty());
        Assert.assertEquals(DATA_SIZE, updateResult.getSuccessData().size());
        Assert.assertEquals(Long.valueOf(DATA_SIZE), countRows());
        Assert.assertEquals("after_1", queryNameById(1L));
        Assert.assertEquals("after_" + DATA_SIZE, queryNameById(DATA_SIZE));
        Assert.assertEquals(Integer.valueOf(88), queryAgeById(DATA_SIZE / 2));
        Assert.assertEquals("updated", queryRemarkById(DATA_SIZE / 2));
        logger.info("批量修改成功, success={}, cost={}ms",
                updateResult.getSuccessData().size(), System.currentTimeMillis() - start);
    }

    @Test
    public void testBatchDelete() {
        long start = System.currentTimeMillis();
        Result insertResult = write(ConnectorConstant.OPERTION_INSERT, buildRows(1, DATA_SIZE, "delete"), false);
        Assert.assertTrue(insertResult.getFailData().isEmpty());
        Assert.assertEquals(Long.valueOf(DATA_SIZE), countRows());

        List<Map> deleteRows = buildRows(1, DATA_SIZE, "delete");
        Result deleteResult = write(ConnectorConstant.OPERTION_DELETE, deleteRows, false);
        Assert.assertTrue(deleteResult.getError().length() == 0 ? null : deleteResult.getError().toString(),
                deleteResult.getFailData().isEmpty());
        Assert.assertEquals(DATA_SIZE, deleteResult.getSuccessData().size());
        Assert.assertEquals(Long.valueOf(0), countRows());
        Assert.assertNull(queryNameById(1L));
        Assert.assertNull(queryNameById(DATA_SIZE));
        logger.info("批量删除成功, remain={}, cost={}ms", countRows(), System.currentTimeMillis() - start);
    }

    private Result write(String event, List<Map> rows, boolean forceUpdate) {
        CommandConfig commandConfig = new CommandConfig(connector.getConnectorType(), DATABASE, table, connectorInstance, null);
        commandConfig.setForceUpdate(forceUpdate);
        Map<String, String> command = connector.getTargetCommand(commandConfig);
        logger.info("event={}, forceUpdate={}, rows={}", event, forceUpdate, rows.size());

        FullPluginContext context = new FullPluginContext();
        context.setTargetConnectorInstance(connectorInstance);
        context.setTargetTable(table);
        context.setTargetFields(fields);
        context.setEvent(event);
        context.setForceUpdate(forceUpdate);
        context.setCommand(command);
        context.setTargetList(rows);
        context.setBatchSize(rows.size());
        return connector.writer(connectorInstance, context);
    }

    private void createTestTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS `" + DATABASE + "`.`" + TABLE_NAME + "` ("
                + "`id` UInt64,"
                + "`name` String,"
                + "`age` Int32,"
                + "`remark` String,"
                + "`update_time` DateTime"
                + ") ENGINE = MergeTree() ORDER BY `id` PRIMARY KEY `id`";
        connectorInstance.execute(template -> {
            template.execute(ddl);
            return null;
        });
        logger.info("创建测试表完成: {}.{}", DATABASE, TABLE_NAME);
    }

    private void truncateTable() {
        // 清理历史卡住的 mutation，避免影响本次删除/覆盖写入
        connectorInstance.execute(template -> {
            try {
                template.execute("KILL MUTATION WHERE database = '" + DATABASE + "' AND table = '" + TABLE_NAME + "'");
            } catch (Exception ignore) {
                // 无待清理 mutation 时忽略
            }
            template.execute("TRUNCATE TABLE IF EXISTS `" + DATABASE + "`.`" + TABLE_NAME + "`");
            return null;
        });
    }

    private Long countRows() {
        return connectorInstance.execute(template ->
                template.queryForObject("SELECT count() FROM `" + DATABASE + "`.`" + TABLE_NAME + "`", Long.class));
    }

    private String queryNameById(long id) {
        List<Map<String, Object>> list = connectorInstance.execute(template ->
                template.queryForList("SELECT `name` FROM `" + DATABASE + "`.`" + TABLE_NAME + "` WHERE `id` = ?", id));
        if (list == null || list.isEmpty() || list.get(0).get("name") == null) {
            return null;
        }
        return String.valueOf(list.get(0).get("name"));
    }

    private Integer queryAgeById(long id) {
        List<Map<String, Object>> list = connectorInstance.execute(template ->
                template.queryForList("SELECT `age` FROM `" + DATABASE + "`.`" + TABLE_NAME + "` WHERE `id` = ?", id));
        if (list == null || list.isEmpty() || list.get(0).get("age") == null) {
            return null;
        }
        return ((Number) list.get(0).get("age")).intValue();
    }

    private String queryRemarkById(long id) {
        List<Map<String, Object>> list = connectorInstance.execute(template ->
                template.queryForList("SELECT `remark` FROM `" + DATABASE + "`.`" + TABLE_NAME + "` WHERE `id` = ?", id));
        if (list == null || list.isEmpty() || list.get(0).get("remark") == null) {
            return null;
        }
        return String.valueOf(list.get(0).get("remark"));
    }

    private List<Field> buildFields() {
        List<Field> list = new ArrayList<>();
        list.add(new Field("id", "UInt64", Types.BIGINT, true));
        list.add(new Field("name", "String", Types.VARCHAR, false));
        list.add(new Field("age", "Int32", Types.INTEGER, false));
        list.add(new Field("remark", "String", Types.VARCHAR, false));
        list.add(new Field("update_time", "DateTime", Types.TIMESTAMP, false));
        return list;
    }

    private List<Map> buildRows(long startId, int size, String namePrefix) {
        List<Map> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long id = startId + i;
            rows.add(row(id, namePrefix + "_" + id, (int) id, namePrefix));
        }
        return rows;
    }

    private Map<String, Object> row(long id, String name, int age, String remark) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("age", age);
        row.put("remark", remark);
        // ClickHouse JDBC 对 UPDATE mutation 会内联参数，Timestamp 无引号会触发语法错误，统一用字符串
        row.put("update_time", LocalDateTime.now().format(DATE_TIME_FORMATTER));
        return row;
    }

    private DatabaseConfig createClickHouseConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setConnectorType("ClickHouse");
        config.setHost("127.0.0.1");
        config.setPort(18123);
        config.setUsername("test");
        config.setPassword("123");
        config.setDatabase(DATABASE);
        config.setMaxActive(64);
        config.setKeepAlive(60000);
        config.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        // mutations_sync=1：保证 UPDATE/DELETE 变更同步可见，便于断言
        config.setProperties(PropertiesUtil.parse(
                "socket_timeout=300000&compress=0&connect_timeout=30000&mutations_sync=1"));
        return config;
    }
}
