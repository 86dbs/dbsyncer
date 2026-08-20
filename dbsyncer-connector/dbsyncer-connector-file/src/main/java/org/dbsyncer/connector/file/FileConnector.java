/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.file.cdc.FileListener;
import org.dbsyncer.connector.file.column.FileResolver;
import org.dbsyncer.connector.file.config.FileConfig;
import org.dbsyncer.connector.file.schema.FileSchemaResolver;
import org.dbsyncer.connector.file.validator.FileConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.ShardSupportEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.shard.ShardPlan;
import org.dbsyncer.sdk.model.shard.ShardPlanRequest;
import org.dbsyncer.sdk.model.shard.ShardPlans;
import org.dbsyncer.sdk.model.shard.ShardSpec;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
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

    public static final String FILE_SEPARATOR = "separator";
    private final String FILE_PATH = "filePath";
    private final FileResolver fileResolver = new FileResolver();
    private final FileConfigValidator configValidator = new FileConfigValidator();
    private final FileSchemaResolver schemaResolver = new FileSchemaResolver();

    @Override
    public String getConnectorType() {
        return "File";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<FileConfig> getConfigClass() {
        return FileConfig.class;
    }

    @Override
    public ConnectorInstance connect(FileConfig config, ConnectorServiceContext context) {
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
        return true;
    }

    @Override
    public List<Table> getTable(FileConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return new ArrayList<>();
    }

    @Override
    public List<MetaInfo> getMetaInfo(FileConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        for (Table table : context.getTablePatterns()) {
            MetaInfo metaInfo = new MetaInfo();
            metaInfo.setTable(table.getName());
            metaInfo.setTableType(getExtendedTableType().getCode());
            metaInfo.setColumn(table.getColumn());
            metaInfo.setExtInfo(table.getExtInfo());
            metaInfos.add(metaInfo);
        }
        return metaInfos;
    }

    @Override
    public long getCount(FileConnectorInstance connectorInstance, MetaContext metaContext) {
        // TODO 待优化，读table扩展信息
        Map<String, String> command = metaContext.getCommand();
        AtomicLong count = new AtomicLong();
        FileReader reader = null;
        try {
            reader = new FileReader(command.get(FILE_PATH));
            LineIterator lineIterator = IOUtils.lineIterator(reader);
            while (lineIterator.hasNext()) {
                lineIterator.next();
                count.addAndGet(1);
            }
        } catch (IOException e) {
            throw new FileException(e);
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
            Map<String, String> command = context.getCommand();
            final List<Field> fields = context.getSourceTable().getColumn();
            Assert.notEmpty(fields, "The fields of file schema is empty.");
            final char separator = command.get(FILE_SEPARATOR).charAt(0);
            reader = new FileReader(command.get(FILE_PATH));
            LineIterator lineIterator = IOUtils.lineIterator(reader);
            long shardFrom = 0L;
            long shardTo = Long.MAX_VALUE;
            ShardSpec shard = context.getShard();
            if (shard != null && shard.getCapability() == ShardSupportEnum.OFFSET) {
                String start = shard.payload(ShardSpec.KEY_OFFSET_START);
                String end = shard.payload(ShardSpec.KEY_OFFSET_END);
                if (NumberUtil.isCreatable(start)
                        && NumberUtil.isCreatable(end)) {
                    shardFrom = NumberUtil.toLong(start);
                    shardTo = NumberUtil.toLong(end);
                }
            }
            int pageFrom = (context.getPageIndex() - 1) * context.getPageSize();
            int pageTo = pageFrom + context.getPageSize();
            long absoluteFrom = shardFrom + pageFrom;
            long absoluteTo = Math.min(shardFrom + pageTo - 1, shardTo);
            AtomicLong count = new AtomicLong();
            while (lineIterator.hasNext()) {
                long line = count.get();
                if (line > shardTo) {
                    break;
                }
                if (line >= absoluteFrom && line <= absoluteTo) {
                    list.add(fileResolver.parseMap(fields, separator, lineIterator.next()));
                } else {
                    lineIterator.next();
                }
                count.addAndGet(1);
                if (line >= absoluteTo) {
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new FileException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return new Result(list);
    }

    @Override
    public ShardPlan planShards(FileConnectorInstance connectorInstance, ShardPlanRequest request) {
        if (request == null || StringUtil.isBlank(request.getTableGroupId()) || connectorInstance == null) {
            return ShardPlan.wholeTable();
        }
        try {
            FileConfig config = connectorInstance.getConfig();
            String fileDir = config == null ? null : config.getFileDir();
            String tableName = request.getSourceTable() == null ? null : request.getSourceTable().getName();
            if (StringUtil.isBlank(fileDir) || StringUtil.isBlank(tableName)) {
                return ShardPlan.wholeTable(request.getTableGroupId());
            }
            File file = new File(fileDir, tableName);
            if (!file.isFile()) {
                return ShardPlan.wholeTable(request.getTableGroupId());
            }
            long total = 0L;
            try (FileReader fr = new FileReader(file)) {
                LineIterator it = IOUtils.lineIterator(fr);
                while (it.hasNext()) {
                    it.next();
                    total++;
                }
            }
            return ShardPlans.offsetByRows(request.getTableGroupId(), total, request.suggestedRangeChunk());
        } catch (Exception e) {
            logger.warn("文件切片失败，回退整表: {}", e.getMessage());
            return ShardPlan.wholeTable(request.getTableGroupId());
        }
    }

    @Override
    public Result writer(FileConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            logger.error("writer data can not be empty.");
            throw new FileException("writer data can not be empty.");
        }

        Result result = new Result();
        OutputStream output = null;
        try {
            Map<String, String> command = context.getCommand();
            final String separator = command.get(FILE_SEPARATOR);
            final String filePath = command.get(FILE_PATH);
            output = new FileOutputStream(filePath, true);
            List<String> lines = data.stream().map(row-> {
                List<String> array = new ArrayList<>();
                context.getTargetFields().forEach(field-> {
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
        Map<String, String> command = getTargetCommand(commandConfig);
        String fieldList = JsonUtil.objToJson(commandConfig.getTable().getColumn());
        command.put(ConnectorConstant.OPERTION_QUERY, fieldList);
        return command;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> command = new HashMap<>();
        FileConfig fileConfig = (FileConfig) commandConfig.getConnectorConfig();
        final String fileDir = fileConfig.getFileDir();
        StringBuilder file = new StringBuilder(fileDir);
        if (!StringUtil.endsWith(fileDir, File.separator)) {
            file.append(File.separator);
        }
        Table table = commandConfig.getTable();
        file.append(table.getName());
        String separator = table.getExtInfo().getProperty(FILE_SEPARATOR, StringUtil.VERTICAL_LINE);
        command.put(FILE_PATH, file.toString());
        command.put(FILE_SEPARATOR, separator);
        return command;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new FileListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}