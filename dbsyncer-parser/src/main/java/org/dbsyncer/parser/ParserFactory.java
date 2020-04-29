package org.dbsyncer.parser;

import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.event.RefreshEvent;
import org.dbsyncer.common.task.Result;
import org.dbsyncer.common.task.Task;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.parser.convert.Convert;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
@Component
public class ParserFactory implements Parser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public boolean alive(ConnectorConfig config) {
        return connectorFactory.isAlive(config);
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return connectorFactory.getTable(config);
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        ConnectorConfig config = getConnectorConfig(connectorId);
        return connectorFactory.getMetaInfo(config, tableName);
    }

    @Override
    public Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup) {
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        if (CollectionUtils.isEmpty(fieldMapping)) {
            return null;
        }
        String sType = getConnectorConfig(sourceConnectorId).getConnectorType();
        String tType = getConnectorConfig(targetConnectorId).getConnectorType();
        String sTableName = tableGroup.getSourceTable().getName();
        String tTableName = tableGroup.getTargetTable().getName();
        Table sTable = new Table().setName(sTableName).setColumn(new ArrayList<>());
        Table tTable = new Table().setName(tTableName).setColumn(new ArrayList<>());
        fieldMapping.forEach(m -> {
            sTable.getColumn().add(m.getSource());
            tTable.getColumn().add(m.getTarget());
        });
        final CommandConfig sourceConfig = new CommandConfig(sType, sTable, tableGroup.getFilter());
        final CommandConfig targetConfig = new CommandConfig(tType, tTable);
        // 获取连接器同步参数
        Map<String, String> command = connectorFactory.getCommand(sourceConfig, targetConfig);
        return command;
    }

    @Override
    public Connector parseConnector(String json) {
        try {
            JSONObject conn = new JSONObject(json);
            JSONObject config = (JSONObject) conn.remove("config");
            Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
            Assert.notNull(connector, "Connector can not be null.");
            String connectorType = config.getString("connectorType");
            Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
            ConnectorConfig obj = (ConnectorConfig) JsonUtil.jsonToObj(config.toString(), configClass);
            connector.setConfig(obj);
            return connector;
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }
    }

    @Override
    public <T> T parseObject(String json, Class<T> clazz) {
        try {
            JSONObject obj = new JSONObject(json);
            T t = JsonUtil.jsonToObj(obj.toString(), clazz);
            String format = String.format("%s can not be null.", clazz.getSimpleName());
            Assert.notNull(t, format);
            return t;
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }
    }

    @Override
    public List<ConnectorEnum> getConnectorEnumAll() {
        return Arrays.asList(ConnectorEnum.values());
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return Arrays.asList(OperationEnum.values());
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
    }

    @Override
    public void execute(Task task, Mapping mapping, TableGroup tableGroup) {
        final String metaId = task.getId();
        final String sourceConnectorId = mapping.getSourceConnectorId();
        final String targetConnectorId = mapping.getTargetConnectorId();

        ConnectorConfig sConfig = getConnectorConfig(sourceConnectorId);
        Assert.notNull(sConfig, "数据源配置不能为空.");
        ConnectorConfig tConfig = getConnectorConfig(targetConnectorId);
        Assert.notNull(tConfig, "目标源配置不能为空.");
        Map<String, String> command = tableGroup.getCommand();
        Assert.notEmpty(command, "执行命令不能为空.");
        List<FieldMapping> fieldMapping = tableGroup.getFieldMapping();
        String sTableName = tableGroup.getSourceTable().getName();
        String tTableName = tableGroup.getTargetTable().getName();
        Assert.notEmpty(fieldMapping, String.format("数据源表[%s]同步到目标源表[%s], 映射关系不能为空.", sTableName, tTableName));
        // 获取同步字段
        Picker picker = new Picker();
        PickerUtil.pickFields(picker, fieldMapping);

        // 转换配置(默认使用全局)
        List<Convert> convert = CollectionUtils.isEmpty(tableGroup.getConvert()) ? mapping.getConvert() : tableGroup.getConvert();
        // 插件配置(默认使用全局)
        Plugin plugin = null == tableGroup.getPlugin() ? mapping.getPlugin() : tableGroup.getPlugin();

        // 检查分页参数
        Map<String, String> params = getMeta(metaId).getMap();
        params.putIfAbsent(ParserEnum.PAGE_INDEX.getCode(), ParserEnum.PAGE_INDEX.getDefaultValue());
        int pageSize = mapping.getBatchNum();
        int threadSize = mapping.getThreadNum();

        for (; ; ) {
            // TODO 模拟测试
            logger.info("模拟迁移5s");
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!task.isRunning()) {
                break;
            }

            // 1、获取数据源数据
            int pageIndex = NumberUtils.toInt(params.get(ParserEnum.PAGE_INDEX.getCode()));
            Result reader = connectorFactory.reader(sConfig, command, pageIndex, pageSize);
            List<Map<String, Object>> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }

            // 2、映射字段
            PickerUtil.pickData(picker, data);

            // 3、参数转换
            List<Map<String, Object>> target = picker.getTarget();
            ConvertUtil.convert(convert, target);

            // 4、插件转换
            pluginFactory.convert(plugin, data, target);

            // 5、写入目标源
            Result writer = connectorFactory.writer(tConfig, command, threadSize, picker.getTargetFields(), target);

            // 6、更新结果
            flush(task, writer, target.size());

            // 7、更新分页数
            params.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(pageIndex++));
        }
        logger.info("完成任务:{}, 表[%s]写入到表[%s]", metaId, sTableName, tTableName);
    }

    /**
     * 更新缓存
     *
     * @param task
     * @param writer
     * @param total
     */
    private void flush(Task task, Result writer, int total) {
        // 引用传递
        long fail = writer.getFail().get();
        long success = total - fail;
        Meta meta = getMeta(task.getId());
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(success);

        // TODO 记录错误日志

        // 发布刷新事件给FullExtractor
        applicationContext.publishEvent(new RefreshEvent(applicationContext, task));
    }

    /**
     * 获取Meta(注: 没有bean拷贝, 便于直接更新缓存)
     *
     * @param metaId
     * @return
     */
    private Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

    /**
     * 获取连接配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        Connector connector = new Connector();
        BeanUtils.copyProperties(conn, connector);
        return connector.getConfig();
    }

}