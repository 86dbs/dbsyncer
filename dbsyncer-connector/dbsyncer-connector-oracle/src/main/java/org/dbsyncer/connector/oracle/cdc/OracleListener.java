/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.logminer.LogMiner;
import org.dbsyncer.connector.oracle.logminer.LogMinerHelper;
import org.dbsyncer.connector.oracle.logminer.RedoEvent;
import org.dbsyncer.connector.oracle.logminer.parser.OracleLobRedoHelper;
import org.dbsyncer.connector.oracle.logminer.parser.OracleUnchangedValue;
import org.dbsyncer.connector.oracle.logminer.parser.impl.DeleteSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.InsertSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.UpdateSql;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public class OracleListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String REDO_POSITION = "position";
    private final Map<String, List<Field>> tableFiledMap = new ConcurrentHashMap<>();
    private LogMiner logMiner;

    @Override
    public void init() {
        super.init();
        sourceTable.forEach(table -> tableFiledMap.put(table.getName(), table.getColumn()));
    }

    @Override
    public void start() {
        try {
            final DatabaseConfig config = getConnectorInstance().getConfig();
            String driverClassName = config.getDriverClassName();
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();
            boolean containsPos = snapshot.containsKey(REDO_POSITION);
            logMiner = new LogMiner(username, password, url, schema, driverClassName);
            logMiner.setStartScn(containsPos ? Long.parseLong(snapshot.get(REDO_POSITION)) : 0);
            logMiner.registerEventListener((event) -> {
                try {
                    parseEvent(event);
                } catch (JSQLParserException e) {
                    logger.warn("不支持sql:{}", event.getRedoSql());
                } catch (Exception e) {
                    logger.error("解析sql异常:{}", event.getRedoSql(), e);
                }
            });
            logMiner.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage(), e);
            throw new OracleException(e);
        }
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            // 如果消费事件失败，重试
            while (logMiner.isConnected()) {
                try {
                    sendChangedEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 解析事件
     *
     * @param event
     * @throws Exception
     */
    private void parseEvent(RedoEvent event) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(event.getRedoSql());
        if (statement instanceof Update) {
            Update update = (Update) statement;
            String tableName = getTableName(update.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                List<Field> fields = tableFiledMap.get(tableName);
                UpdateSql parser = new UpdateSql(update, fields);
                List<Object> data = parser.parseColumns();
                // 缺列哨兵 + 大 LOB 的 EMPTY_BLOB/EMPTY_CLOB 占位，均需按主键回查实值
                List<Integer> indexes = findUpdateColumnsNeedFill(fields, data);
                if (!fillColumnsByPrimaryKey(tableName, fields, data, indexes)) {
                    logger.error("UPDATE 回查源表失败，丢弃事件以免写入空 LOB. table={}, scn={}, sql={}",
                            tableName, event.getScn(), event.getRedoSql());
                    return;
                }
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, data, null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            String tableName = getTableName(insert.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                List<Field> fields = tableFiledMap.get(tableName);
                InsertSql parser = new InsertSql(insert, fields);
                List<Object> data = parser.parseColumns();
                List<Integer> lobIndexes = OracleLobRedoHelper.findEmptyLobPlaceholderIndexes(fields, data);
                if (!fillColumnsByPrimaryKey(tableName, fields, data, lobIndexes)) {
                    logger.error("INSERT 回查源表 LOB 失败，丢弃事件以免写入空 LOB. table={}, scn={}, sql={}",
                            tableName, event.getScn(), event.getRedoSql());
                    return;
                }
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_INSERT, data, null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            String tableName = getTableName(delete.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                DeleteSql parser = new DeleteSql(delete, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_DELETE, parser.parseColumns(), null, event.getScn()));
            }
        }

        if (statement instanceof Alter) {
            Alter alter = (Alter) statement;
            String tableName = getTableName(alter.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                logger.info("sql:{}", event.getRedoSql());
                trySendEvent(new DDLChangedEvent(tableName, ConnectorConstant.OPERTION_ALTER, event.getRedoSql(), null, event.getScn()));
            }
        }
    }

    /**
     * 找出 UPDATE redo 中未出现的列下标（{@link OracleUnchangedValue} 哨兵）。
     * <p>缺列若当 null 写入，全字段 MERGE 会把目标侧未变更的 LOB 清空。
     */
    private List<Integer> findUnchangedIndexes(List<Object> data) {
        List<Integer> indexes = new ArrayList<>();
        if (CollectionUtils.isEmpty(data)) {
            return indexes;
        }
        for (int i = 0; i < data.size(); i++) {
            if (OracleUnchangedValue.isUnchanged(data.get(i))) {
                indexes.add(i);
            }
        }
        return indexes;
    }

    /**
     * UPDATE 需回查的列：缺列哨兵 + LogMiner 对大 LOB 常用的 EMPTY_BLOB()/EMPTY_CLOB() 占位。
     * <p>仅改 其他类型是 时 LOB 为哨兵；直接改大 LOB 时 SET 侧多为 EMPTY_*()，二者都要回表取实值。
     */
    private List<Integer> findUpdateColumnsNeedFill(List<Field> fields, List<Object> data) {
        List<Integer> indexes = findUnchangedIndexes(data);
        for (Integer idx : OracleLobRedoHelper.findEmptyLobPlaceholderIndexes(fields, data)) {
            if (!indexes.contains(idx)) {
                indexes.add(idx);
            }
        }
        return indexes;
    }

    /**
     * 按主键回查源表，用实值覆盖指定列下标（UPDATE 缺列哨兵 / INSERT LOB 占位）。
     *
     * @param indexes 需要回填的列下标；为空则直接成功
     * @return false 表示需要回查但失败，调用方应丢弃事件
     */
    private boolean fillColumnsByPrimaryKey(String tableName, List<Field> fields, List<Object> data, List<Integer> indexes) {
        // 无需回查（普通小 UPDATE / 无 LOB 占位的 INSERT）
        if (CollectionUtils.isEmpty(indexes)) {
            return true;
        }
        if (CollectionUtils.isEmpty(data) || CollectionUtils.isEmpty(fields) || data.size() != fields.size()) {
            return false;
        }

        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(fields);
        if (CollectionUtils.isEmpty(pkFields)) {
            logger.error("表 {} 无主键，无法回查列", tableName);
            return false;
        }

        // 主键必须出现在 redo 中，否则无法定位源行
        Object[] pkArgs = buildPrimaryKeyArgs(tableName, fields, data, pkFields);
        if (pkArgs == null) {
            return false;
        }

        Database database = (Database) connectorService;
        // 只 SELECT 待回填列，避免宽表/多 LOB 时整行拉取
        String sql = buildSelectSqlByPrimaryKey(database, tableName, fields, indexes, pkFields);
        if (sql == null) {
            return false;
        }

        try {
            Map<String, Object> row = getConnectorInstance().execute(databaseTemplate -> {
                try {
                    return databaseTemplate.queryForMap(sql, pkArgs);
                } catch (org.springframework.dao.EmptyResultDataAccessException e) {
                    // 行已删或不存在：与查询异常区分，上层统一判空后丢弃事件
                    return null;
                }
            });
            if (CollectionUtils.isEmpty(row)) {
                logger.error("表 {} 按主键回查无数据, args={}", tableName, pkArgs);
                return false;
            }
            // 定点覆盖：保留 redo 已解析出的真实值（含显式 NULL），只补哨兵/占位列
            for (Integer idx : indexes) {
                Field field = fields.get(idx);
                data.set(idx, row.get(field.getName()));
            }
            return true;
        } catch (Exception e) {
            logger.error("表 {} 回查列失败: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 从 redo 行数据组装主键绑定参数；主键缺失或仍为哨兵时返回 null。
     */
    private Object[] buildPrimaryKeyArgs(String tableName, List<Field> fields, List<Object> data, List<Field> pkFields) {
        Object[] args = new Object[pkFields.size()];
        for (int i = 0; i < pkFields.size(); i++) {
            Field pk = pkFields.get(i);
            int idx = indexOfField(fields, pk.getName());
            if (idx < 0 || OracleUnchangedValue.isUnchanged(data.get(idx))) {
                logger.error("表 {} 主键 {} 在 redo 中缺失，无法回查", tableName, pk.getName());
                return null;
            }
            args[i] = data.get(idx);
        }
        return args;
    }

    /**
     * 拼装按主键回查 SQL：仅包含 indexes 对应列。
     *
     * @return 非法下标时返回 null
     */
    private String buildSelectSqlByPrimaryKey(Database database, String tableName, List<Field> fields,
                                              List<Integer> indexes, List<Field> pkFields) {
        List<String> selectCols = new ArrayList<>(indexes.size());
        for (Integer idx : indexes) {
            if (idx == null || idx < 0 || idx >= fields.size()) {
                logger.error("表 {} 回填列下标非法: {}", tableName, idx);
                return null;
            }
            selectCols.add(database.buildWithQuotation(fields.get(idx).getName()));
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(StringUtil.join(selectCols, StringUtil.COMMA));
        sql.append(" FROM ");
        if (StringUtil.isNotBlank(schema)) {
            sql.append(database.buildWithQuotation(schema)).append(StringUtil.POINT);
        }
        sql.append(database.buildWithQuotation(tableName)).append(" WHERE ");
        List<String> pkNames = pkFields.stream().map(Field::getName).collect(Collectors.toList());
        database.appendPrimaryKeys(sql, pkNames);
        return sql.toString();
    }

    private int indexOfField(List<Field> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (StringUtil.equals(fields.get(i).getName(), name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public Map<String, String> captureSnapshot() {
        try {
            final DatabaseConfig config = getConnectorInstance().getConfig();
            try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())) {
                long scn = LogMinerHelper.getCurrentScn(connection);
                snapshot.put(REDO_POSITION, String.valueOf(scn));
                Map<String, String> captured = new HashMap<>(1);
                captured.put(REDO_POSITION, String.valueOf(scn));
                return captured;
            }
        } catch (Exception e) {
            logger.error("捕获Oracle SCN位点失败:{}", e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        if (logMiner != null) {
            logMiner.close();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        snapshot.put(REDO_POSITION, String.valueOf(offset.getPosition()));
    }

    private String getTableName(Table table) {
        return table == null ? StringUtil.EMPTY : StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }

}
