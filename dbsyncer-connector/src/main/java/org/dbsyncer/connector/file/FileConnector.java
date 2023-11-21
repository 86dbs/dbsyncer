package org.dbsyncer.connector.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.config.WriterBatchConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
@Component
public final class FileConnector extends AbstractConnector implements ConnectorService<FileConnectorInstance, FileConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String TYPE = "File";
    private final String FILE_NAME = "fileName";
    private final String FILE_PATH = "filePath";
    private final FileResolver fileResolver = new FileResolver();

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @Override
    public boolean isSupportedTiming() {
        return false;
    }

    @Override
    public boolean isSupportedLog() {
        return true;
    }

    @Override
    public Class<FileConfig> getConfigClass() {
        return FileConfig.class;
    }

    @Override
    public ConnectorInstance connect(FileConfig config) {
        return new FileConnectorInstance(config);
    }

    @Override
    public void disconnect(FileConnectorInstance connectorInstance) {

    }

    @Override
    public boolean isAlive(FileConnectorInstance connectorInstance) {
        String fileDir = connectorInstance.getConnection();
        boolean alive = new File(fileDir).exists();
        if (!alive) {
            logger.warn("can not find fileDir:{}", fileDir);
            return false;
        }
        for (FileSchema fileSchema : connectorInstance.getFileSchemaList()) {
            String filePath = connectorInstance.getFilePath(fileSchema.getFileName());
            if (!new File(filePath).exists()) {
                logger.warn("can not find file:{}", filePath);
                alive = false;
            }
        }
        return alive;
    }

    @Override
    public String getConnectorInstanceCacheKey(FileConfig config) {
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
    public List<Table> getTable(FileConnectorInstance connectorInstance) {
        return connectorInstance.getFileSchemaList().stream().map(fileSchema -> new Table(fileSchema.getFileName())).collect(Collectors.toList());
    }

    @Override
    public MetaInfo getMetaInfo(FileConnectorInstance connectorInstance, String tableName) {
        FileSchema fileSchema = connectorInstance.getFileSchema(tableName);
        return new MetaInfo().setColumn(fileSchema.getFields());
    }

    @Override
    public long getCount(FileConnectorInstance connectorInstance, Map<String, String> command) {
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
    public Result reader(FileConnectorInstance connectorInstance, ReaderConfig config) {
        List<Map<String, Object>> list = new ArrayList<>();
        FileReader reader = null;
        try {
            FileConfig fileConfig = connectorInstance.getConfig();
            FileSchema fileSchema = connectorInstance.getFileSchema(config.getCommand().get(FILE_NAME));
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
    public Result writer(FileConnectorInstance connectorInstance, WriterBatchConfig config) {
        List<Map> data = config.getData();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new ConnectorException("writer data can not be empty.");
        }

        final List<Field> fields = config.getFields();
        final String separator = new String(new char[]{connectorInstance.getConfig().getSeparator()});

        Result result = new Result();
        OutputStream output = null;
        try {
            final String filePath = connectorInstance.getFilePath(config.getCommand().get(FILE_NAME));
            output = new FileOutputStream(filePath, true);
            List<String> lines = data.stream().map(row -> {
                List<String> array = new ArrayList<>();
                fields.forEach(field -> {
                    Object o = row.get(field.getName());
                    array.add(null != o ? String.valueOf(o) : "");
                });
                return StringUtil.join(array.toArray(), separator);
            }).collect(Collectors.toList());
            IOUtils.writeLines(lines, System.lineSeparator(), output, "UTF-8");
        } catch (Exception e) {
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(output);
        }
        return result;
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
        Map<String, String> command = new HashMap<>();
        command.put(FILE_NAME, commandConfig.getTable().getName());
        return command;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new FileListener();
        }
        return null;
    }

}