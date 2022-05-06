package org.dbsyncer.connector.file;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.AbstractConnector;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.model.FileSchema;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
        return connectorMapper.getConnection().exists();
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
        return String.format("%s-%s", localIP, config.getFileDir());
    }

    @Override
    public List<Table> getTable(FileConnectorMapper connectorMapper) {
        return getFileSchema(connectorMapper).stream().map(fileSchema -> new Table(fileSchema.getFileName())).collect(Collectors.toList());
    }

    @Override
    public MetaInfo getMetaInfo(FileConnectorMapper connectorMapper, String tableName) {
        MetaInfo metaInfo = new MetaInfo();
        List<FileSchema> fileSchemas = getFileSchema(connectorMapper);
        for (FileSchema fileSchema : fileSchemas) {
            if (StringUtil.equals(fileSchema.getFileName(), tableName)) {
                metaInfo.setColumn(fileSchema.getFields());
                break;
            }
        }
        return metaInfo;
    }

    @Override
    public long getCount(FileConnectorMapper connectorMapper, Map<String, String> command) {
        AtomicLong count = new AtomicLong();
        final String fileDir = connectorMapper.getConfig().getFileDir();
        getFileSchema(connectorMapper).forEach(fileSchema -> {
            StringBuilder file = new StringBuilder(fileDir);
            if (!StringUtil.endsWith(fileDir, File.separator)) {
                file.append(File.separator);
            }
            file.append(fileSchema.getFileName());

            try {
                List<String> lines = FileUtils.readLines(new File(file.toString()), Charset.defaultCharset());
                count.addAndGet(lines.size());
            } catch (IOException e) {
                throw new ConnectorException(e.getCause());
            }
        });
        return count.get();
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
        return Collections.EMPTY_MAP;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return Collections.EMPTY_MAP;
    }

    private List<FileSchema> getFileSchema(FileConnectorMapper connectorMapper) {
        List<FileSchema> fileSchemas = JsonUtil.jsonToArray(connectorMapper.getConfig().getSchema(), FileSchema.class);
        Assert.notEmpty(fileSchemas, "The schema is empty.");
        return fileSchemas;
    }
}
