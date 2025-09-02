/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.checker;

import org.dbsyncer.biz.enums.SafeInfoEnum;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Convert;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.model.Plugin;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SYMBOL = "***";

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    /**
     * 修改基本配置
     * <p>id,type,name,createTime,updateTime</p>
     *
     * @param model
     * @param params
     */
    protected void modifyConfigModel(ConfigModel model, Map<String, String> params) {
        Assert.notNull(model, "ConfigModel can not be null.");
        Assert.hasText(model.getType(), "ConfigModel type can not be empty.");
        Assert.hasText(model.getName(), "ConfigModel name can not be empty.");
        // 名称
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        if (StringUtil.isNotBlank(name)) {
            model.setName(name);
        }
        model.setId(StringUtil.isBlank(model.getId()) ? String.valueOf(snowflakeIdWorker.nextId()) : model.getId());
        long now = Instant.now().toEpochMilli();
        model.setCreateTime(null == model.getCreateTime() ? now : model.getCreateTime());
        model.setUpdateTime(now);
    }

    /**
     * 修改高级配置：过滤条件/转换配置/插件配置
     *
     * @param model
     * @param params
     */
    protected void modifySuperConfigModel(AbstractConfigModel model, Map<String, String> params) {
        // 全局参数
        String mappingParams = params.get("commonParams");
        Map<String, String> paramMap = StringUtil.isNotBlank(mappingParams) ? JsonUtil.jsonToObj(mappingParams, Map.class) : new ConcurrentHashMap<>();
        model.setParams(paramMap);

        // 过滤条件
        String filterJson = params.get("filter");
        if (StringUtil.isNotBlank(filterJson)) {
            List<Filter> list = JsonUtil.jsonToArray(filterJson, Filter.class);
            model.setFilter(list);
        }

        // 转换配置
        String convertJson = params.get("convert");
        if (StringUtil.isNotBlank(convertJson)) {
            List<Convert> convert = JsonUtil.jsonToArray(convertJson, Convert.class);
            model.setConvert(convert);
        }

        // 插件配置
        String pluginId = params.get("pluginId");
        Plugin plugin = null;
        if (StringUtil.isNotBlank(pluginId)) {
            List<Plugin> plugins = pluginFactory.getPluginAll();
            if (!CollectionUtils.isEmpty(plugins)) {
                for (Plugin p : plugins) {
                    if (StringUtil.equals(pluginFactory.createPluginId(p.getClassName(), p.getVersion()), pluginId)) {
                        plugin = p;
                        break;
                    }
                }
            }
        }
        model.setPlugin(plugin);
        // 插件参数
        model.setPluginExtInfo(plugin != null ? params.get("pluginExtInfo") : null);
    }

    /**
     * 打印参数（数据脱敏）
     *
     * @param params
     */
    protected void printParams(Map<String, String> params) {
        Map<Object, Object> checkParams = new HashMap<>(params);
        for (SafeInfoEnum s : SafeInfoEnum.values()) {
            if (params.containsKey(s.getCode())) {
                checkParams.put(s.getCode(), SYMBOL);
            }
        }
        logger.info("params:{}", checkParams);
    }

}