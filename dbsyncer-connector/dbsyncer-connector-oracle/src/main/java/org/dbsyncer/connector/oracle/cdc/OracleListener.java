/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.logminer.parser.AbstractParser;
import org.dbsyncer.connector.oracle.logminer.LogMiner;
import org.dbsyncer.connector.oracle.logminer.RedoEvent;
import org.dbsyncer.connector.oracle.logminer.parser.impl.DeleteSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.InsertSql;
import org.dbsyncer.connector.oracle.logminer.parser.impl.UpdateSql;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
            // TODO [increment-worker-1184659161326161921] 这里应该单独启动一个线程，线程名要有一定意义，如:binlog-parser-127.0.0.1:3306_123，便于监控排查问题
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

    private void trySendEvent(RowChangedEvent event){
        try {
            // 如果消费事件失败，重试
            long now = Instant.now().toEpochMilli();
            boolean isReTry = false;
            while (logMiner.isConnected()){
                try {
                    sendChangedEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    isReTry = true;
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
            if (isReTry) {
                logger.info("重试耗时：{}ms", Instant.now().toEpochMilli() - now);
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
        // TODO life 注意拦截子查询, 或修改主键值情况
        Statement statement = CCJSqlParserUtil.parse(event.getRedoSql());
        if (statement instanceof Update) {
            Update update = (Update) statement;
            UpdateSql parser = new UpdateSql(update);
            setTable(parser, update.getTable());
            trySendEvent(new RowChangedEvent(parser.getTableName(), ConnectorConstant.OPERTION_UPDATE, parser.parseColumns(), null, event.getScn()));
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            InsertSql parser = new InsertSql(insert);
            setTable(parser, insert.getTable());
            trySendEvent(new RowChangedEvent(parser.getTableName(), ConnectorConstant.OPERTION_INSERT, parser.parseColumns(), null, event.getScn()));
            return;
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            DeleteSql parser = new DeleteSql(delete);
            setTable(parser, delete.getTable());
            trySendEvent(new RowChangedEvent(parser.getTableName(), ConnectorConstant.OPERTION_DELETE, parser.parseColumns(), null, event.getScn()));
        }

        // TODO ddl
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

    private AbstractParser setTable(AbstractParser parser, Table table) {
        parser.setTableName(table == null ? StringUtil.EMPTY : StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY));
        parser.setFields(tableFiledMap.get(parser.getTableName()));
        return parser;
    }

}