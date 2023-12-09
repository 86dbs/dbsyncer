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
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.logminer.LogMiner;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.SqlChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public class OracleListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String REDO_POSITION = "position";

    private LogMiner logMiner;

    @Override
    public void start() {
        try {
            final DatabaseConfig config = (DatabaseConfig) connectorConfig;
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
                    Statement statement = CCJSqlParserUtil.parse(event.getRedoSql());
                    if (statement instanceof Update) {
                        Update update = (Update) statement;
                        sendChangedEvent(new SqlChangedEvent(replaceTableName(update.getTable()), ConnectorConstant.OPERTION_UPDATE, event.getRedoSql(), null, event.getScn()));
                        return;
                    }

                    if (statement instanceof Insert) {
                        Insert insert = (Insert) statement;
                        sendChangedEvent(new SqlChangedEvent(replaceTableName(insert.getTable()), ConnectorConstant.OPERTION_INSERT, event.getRedoSql(), null, event.getScn()));
                        return;
                    }

                    if (statement instanceof Delete) {
                        Delete delete = (Delete) statement;
                        sendChangedEvent(new SqlChangedEvent(replaceTableName(delete.getTable()), ConnectorConstant.OPERTION_DELETE, event.getRedoSql(), null, event.getScn()));
                        return;
                    }

                    if (statement instanceof Alter) {
                        Alter alter = (Alter) statement;
                        sendChangedEvent(new DDLChangedEvent("", replaceTableName(alter.getTable()), ConnectorConstant.OPERTION_ALTER, event.getRedoSql(), null, event.getScn()));
                        return;
                    }
                } catch (JSQLParserException e) {
                    logger.error("parse redoSql=" + event.getRedoSql(), e);
                }
            });
            logMiner.start();

        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new OracleException(e);
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

    private String replaceTableName(Table table) {
        if (table == null) {
            return StringUtil.EMPTY;
        }
        return StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }

}