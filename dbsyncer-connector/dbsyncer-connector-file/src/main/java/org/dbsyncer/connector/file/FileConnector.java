/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.file.cdc.FileListener;
import org.dbsyncer.connector.file.config.FileConfig;
import org.dbsyncer.connector.file.model.FileResolver;
import org.dbsyncer.connector.file.model.FileSchema;
import org.dbsyncer.connector.file.validator.FileConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 文件连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public final class FileConnector extends AbstractConnector implements ConnectorService<FileConnectorInstance, FileConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String FILE_NAME = "fileName";
    private final String FILE_PATH = "filePath";
    private final FileResolver fileResolver = new FileResolver();
    private final FileConfigValidator configValidator = new FileConfigValidator();

    public FileConnector() {
        VALUE_MAPPERS.put(Types.BIT, new FileBitValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "File";
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
    public ConfigValidator getConfigValidator() {
        return configValidator;
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
            throw new FileException(e.getCause());
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return count.get();
    }

    @Override
    public Result reader(FileConnectorInstance connectorInstance, ReaderContext context) {
        List<Map<String, Object>> list = new ArrayList<>();
        FileReader reader = null;
        try {
            FileConfig fileConfig = connectorInstance.getConfig();
            FileSchema fileSchema = connectorInstance.getFileSchema(context.getCommand().get(FILE_NAME));
            final List<Field> fields = fileSchema.getFields();
            Assert.notEmpty(fields, "The fields of file schema is empty.");
            final char separator = fileConfig.getSeparator();

            reader = new FileReader(context.getCommand().get(FILE_PATH));
            LineIterator lineIterator = IOUtils.lineIterator(reader);
            int from = (context.getPageIndex() - 1) * context.getPageSize();
            int to = from + context.getPageSize();
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
            throw new FileException(e.getCause());
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return new Result(list);
    }

    @Override
    public Result writer(FileConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new FileException("writer data can not be empty.");
        }

        final String separator = new String(new char[]{connectorInstance.getConfig().getSeparator()});

        Result result = new Result();
        OutputStream output = null;
        try {
            final String filePath = connectorInstance.getFilePath(context.getCommand().get(FILE_NAME));
            output = new FileOutputStream(filePath, true);
            List<String> lines = data.stream().map(row -> {
                List<String> array = new ArrayList<>();
                context.getTargetFields().forEach(field -> {
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