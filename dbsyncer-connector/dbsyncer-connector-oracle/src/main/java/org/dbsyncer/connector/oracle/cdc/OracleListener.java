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
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.logminer.LogMiner;
import org.dbsyncer.connector.oracle.logminer.RedoEvent;
import org.dbsyncer.connector.oracle.logminer.parser.impl.DeleteSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.InsertSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.UpdateSql;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
            String schema = config.getSchema();
            boolean containsPos = snapshot.containsKey(REDO_POSITION);
            logMiner = new LogMiner(username, password, url, schema, driverClassName);
            logMiner.setStartScn(containsPos ? Long.parseLong(snapshot.get(REDO_POSITION)) : 0);
            logMiner.registerEventListener((event) -> {
                try {
                    parseEvent(event);
                } catch (JSQLParserException e) {
                    logger.error("不支持sql:{}", event.getRedoSql());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
            logMiner.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
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
                UpdateSql parser = new UpdateSql(update, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            String tableName = getTableName(insert.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                InsertSql parser = new InsertSql(insert, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_INSERT, parser.parseColumns(), null, event.getScn()));
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

    @Override
    public void close() {
        try {
            if (logMiner != null) {
                logMiner.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
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