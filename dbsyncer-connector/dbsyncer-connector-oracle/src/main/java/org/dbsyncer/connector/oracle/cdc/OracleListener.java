/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import java.sql.SQLException;
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
import org.dbsyncer.connector.oracle.logminer.RedoEvent;
import org.dbsyncer.connector.oracle.logminer.parser.DeleteSql;
import org.dbsyncer.connector.oracle.logminer.parser.InsertSql;
import org.dbsyncer.connector.oracle.logminer.parser.Parser;
import org.dbsyncer.connector.oracle.logminer.parser.UpdateSql;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractDatabaseListener;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.listener.event.SqlChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            if (!snapshot.get(REDO_POSITION).equals("null")){
                logMiner.setStartScn(containsPos ? Long.parseLong(snapshot.get(REDO_POSITION)) : 0);
            }
            logMiner.registerEventListener((event) -> {
//                sendSql(event);
                try {
                    parseSqlToPk(event);
                } catch (JSQLParserException e) {
                    logger.error("不支持sql:" + event.getRedoSql());
                }catch (Exception e){
                    logger.error(e.getMessage());
                }
            });
            logMiner.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new OracleException(e);
        }
    }

    //发送sql解析时间
    private void sendSql(RedoEvent event){
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
            logger.error("不支持sql:" + event.getRedoSql());
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    //解析sql出来主键数据
    private void parseSqlToPk(RedoEvent event) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(event.getRedoSql());
        if (statement instanceof Insert){
            Insert insert = (Insert) statement;
            org.dbsyncer.sdk.model.Table table1 =sourceTable.stream()
                    .filter(x->x.getName().equals(replaceTableName(insert.getTable())))
                    .findFirst().orElse(null);
            if (table1 != null){
                Parser parser = new InsertSql(insert,table1.getColumn(),(DatabaseConnectorInstance) connectorInstance);
                sendChangedEvent(new RowChangedEvent(replaceTableName(insert.getTable()), ConnectorConstant.OPERTION_INSERT,parser.parseSql() ,null,event.getScn()));
            }

        }else if (statement instanceof Update){
            Update update = (Update) statement;
            org.dbsyncer.sdk.model.Table table1 =sourceTable.stream()
                    .filter(x->x.getName().equals(replaceTableName(update.getTable())))
                    .findFirst().orElse(null);
            if (table1 != null){
                Parser parser = new UpdateSql(update,table1.getColumn(),(DatabaseConnectorInstance) connectorInstance);
                sendChangedEvent(new RowChangedEvent(replaceTableName(update.getTable()), ConnectorConstant.OPERTION_UPDATE, parser.parseSql(),null,event.getScn()));
            }
        }else if (statement instanceof Delete){
            Delete delete = (Delete) statement;
            org.dbsyncer.sdk.model.Table table1 =sourceTable.stream()
                    .filter(x->x.getName().equals(replaceTableName(delete.getTable())))
                    .findFirst().orElse(null);
            if (table1 !=null){
                Parser parser = new DeleteSql(delete,table1.getColumn());
                sendChangedEvent(new RowChangedEvent(replaceTableName(delete.getTable()), ConnectorConstant.OPERTION_DELETE, parser.parseSql(),null,event.getScn()));
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

    private String replaceTableName(Table table) {
        if (table == null) {
            return StringUtil.EMPTY;
        }
        return StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }


}