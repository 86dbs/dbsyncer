package org.dbsyncer.connector.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
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
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.FileSchema;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnector extends AbstractConnector implements Connector<FileConnectorMapper, FileConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String FILE_NAME = "fileName";
    private static final String FILE_PATH = "filePath";
    private final FileResolver fileResolver = new FileResolver();

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
        FileSchema fileSchema = getFileSchema(connectorMapper, tableName);
        return new MetaInfo().setColumn(fileSchema.getFields());
    }

    @Override
    public long getCount(FileConnectorMapper connectorMapper, Map<String, String> command) {
        AtomicLong count = new AtomicLong();
        FileReader reader = null;
        try {
            reader = new FileReader(new File(command.get(FILE_PATH)));
            LineIterator lineIterator = IOUtils.lineIterator(reader);
            while (lineIterator.hasNext()) {
                lineIterator.next();
                count.addAndGet(1);
            }
        } catch (IOException e) {
            throw new ConnectorException(e.getCause());
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return count.get();
    }

    @Override
    public Result reader(FileConnectorMapper connectorMapper, ReaderConfig config) {
        List<Map<String, Object>> list = new ArrayList<>();
        FileReader reader = null;
        try {
            FileConfig fileConfig = connectorMapper.getConfig();
            FileSchema fileSchema = getFileSchema(connectorMapper, config.getCommand().get(FILE_NAME));
            final List<Field> fields = fileSchema.getFields();
            Assert.notEmpty(fields, "The fields of file schema is empty.");
            final char separator = fileConfig.getSeparator();

            reader = new FileReader(new File(config.getCommand().get(FILE_PATH)));
            LineIterator lineIterator = IOUtils.lineIterator(reader);
            int from = (config.getPageIndex() - 1) * config.getPageSize();
            int to = from + config.getPageSize();
            AtomicLong count = new AtomicLong();
            while (lineIterator.hasNext()) {
                if (count.get() >= from && count.get() < to) {
                    list.add(fileResolver.parseMap(fields, separator, lineIterator.next()));
                } else {
                    lineIterator.next();
                }
                count.addAndGet(1);
                if (count.get() >= to) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new ConnectorException(e.getCause());
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return new Result(list);
    }

    @Override
    public Result writer(FileConnectorMapper connectorMapper, WriterBatchConfig config) {
        return null;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        Map<String, String> command = new HashMap<>();
        FileConfig fileConfig = (FileConfig) commandConfig.getConnectorConfig();
        final String fileDir = fileConfig.getFileDir();
        StringBuilder file = new StringBuilder(fileDir);
        if (!StringUtil.endsWith(fileDir, File.separator)) {
            file.append(File.separator);
        }
        file.append(commandConfig.getTable().getName());
        command.put(FILE_PATH, file.toString());
        command.put(FILE_NAME, commandConfig.getTable().getName());
        return command;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        return Collections.EMPTY_MAP;
    }

    private FileSchema getFileSchema(FileConnectorMapper connectorMapper, String tableName) {
        List<FileSchema> fileSchemaList = getFileSchema(connectorMapper);
        for (FileSchema fileSchema : fileSchemaList) {
            if (StringUtil.equals(fileSchema.getFileName(), tableName)) {
                return fileSchema;
            }
        }
        throw new ConnectorException(String.format("can not find fileSchema by tableName '%s'", tableName));
    }

    private List<FileSchema> getFileSchema(FileConnectorMapper connectorMapper) {
        List<FileSchema> fileSchemas = JsonUtil.jsonToArray(connectorMapper.getConfig().getSchema(), FileSchema.class);
        Assert.notEmpty(fileSchemas, "The schema is empty.");
        return fileSchemas;
    }
}
