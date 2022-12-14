package org.dbsyncer.biz.checker;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.dbsyncer.biz.enums.SafeInfoEnum;
import org.dbsyncer.common.snowflake.SnowflakeIdWorker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Convert;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SYMBOL = "***";

    @Autowired
    private Manager manager;

    @Autowired
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
        String mappingParams = params.get("params");
        model.setParams(StringUtil.isNotBlank(mappingParams) ? JsonUtil.jsonToObj(mappingParams, Map.class) : new LinkedHashMap());

        // 过滤条件
        String filterJson = params.get("filter");
        if (StringUtil.isNotBlank(filterJson)) {
            List<Filter> list = jsonToList(filterJson, Filter.class);
            model.setFilter(list);
        }

        // 转换配置
        String convertJson = params.get("convert");
        if (StringUtil.isNotBlank(convertJson)) {
            List<Convert> convert = jsonToList(convertJson, Convert.class);
            model.setConvert(convert);
        }

        // 插件配置
        String pluginClassName = params.get("pluginClassName");
        Plugin plugin = null;
        if (StringUtil.isNotBlank(pluginClassName)) {
            List<Plugin> plugins = manager.getPluginAll();
            if (!CollectionUtils.isEmpty(plugins)) {
                for (Plugin p : plugins) {
                    if (StringUtil.equals(p.getClassName(), pluginClassName)) {
                        plugin = p;
                        break;
                    }
                }
            }
        }
        model.setPlugin(plugin);
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

    private <T> List<T> jsonToList(String json, Class<T> valueType) {
        JSONArray array = JsonUtil.parseArray(json);
        if (null != array) {
            List<T> list = new ArrayList<>();
            int length = array.size();
            for (int i = 0; i < length; i++) {
                JSONObject obj = array.getJSONObject(i);
                T t = JsonUtil.jsonToObj(obj.toString(), valueType);
                list.add(t);
            }
            return list;
        }
        return Collections.EMPTY_LIST;
    }

}