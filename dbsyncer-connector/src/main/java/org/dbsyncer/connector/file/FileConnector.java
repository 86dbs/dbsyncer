package org.dbsyncer.connector.file;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnector extends AbstractConnector implements Connector<FileConnectorMapper, FileConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public ConnectorMapper connect(FileConfig config) {
        return new FileConnectorMapper(config);
    }

    @Override
    public void disconnect(FileConnectorMapper connectorMapper) {

    }

    @Override
    public boolean isAlive(FileConnectorMapper connectorMapper) {
        return false;
    }

    @Override
    public String getConnectorMapperCacheKey(FileConfig config) {
        String localIP;
        try {
            localIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
            localIP = "127.0.0.1";
        }
        return String.format("%s-%s", config.getConnectorType(), localIP, config.getFileDir());
    }

    @Override
    public List<Table> getTable(FileConnectorMapper connectorMapper) {
        return null;
    }

    @Override
    public MetaInfo getMetaInfo(FileConnectorMapper connectorMapper, String tableName) {
        return null;
    }

    @Override
    public long getCount(FileConnectorMapper connectorMapper, Map<String, String> command) {
        return 0;
    }

    @Override
    public Result reader(FileConnectorMapper connectorMapper, ReaderConfig config) {
        return null;
    }

    @Override
    public Result writer(FileConnectorMapper connectorMapper, WriterBatchConfig config) {
        return null;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return null;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return null;
    }
}
