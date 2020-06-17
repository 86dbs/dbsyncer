package org.dbsyncer.listener.oracle;

import oracle.jdbc.dcn.TableChangeDescription;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.oracle.dcn.DBChangeNotification;
import org.dbsyncer.listener.oracle.dcn.RowChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class OracleExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DBChangeNotification client;

    @Override
    public void start() {
        try {
            final DatabaseConfig config = (DatabaseConfig) connectorConfig;
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();
            client = new DBChangeNotification(username, password, url);
            client.addRowEventListener((e) -> onEvent(e));
            client.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        }
    }

    @Override
    public void close() {
        if(null != client){
            client.close();
        }
    }

    private void onEvent(RowChangeEvent event){
        if(event.getEvent() == TableChangeDescription.TableOperation.UPDATE.getCode()){
            changedLogEvent(event.getTableName(), ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, event.getData());
            return;
        }

        if(event.getEvent() == TableChangeDescription.TableOperation.INSERT.getCode()){
            changedLogEvent(event.getTableName(), ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, event.getData());
            return;
        }

        if(event.getEvent() == TableChangeDescription.TableOperation.DELETE.getCode()){
            changedLogEvent(event.getTableName(), ConnectorConstant.OPERTION_DELETE, event.getData(), Collections.EMPTY_LIST);
            return;
        }
    }

}