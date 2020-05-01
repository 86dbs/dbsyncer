package org.dbsyncer.biz.checker;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.model.Convert;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.SnowflakeIdWorker;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    @Autowired
    private PluginService pluginService;

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
        if (StringUtils.isNotBlank(name)) {
            model.setName(name);
        }
        model.setId(StringUtils.isEmpty(model.getId()) ? String.valueOf(snowflakeIdWorker.nextId()) : model.getId());
        long now = System.currentTimeMillis();
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
        // 过滤条件
        String filterJson = params.get("filter");
        if (StringUtils.isNotBlank(filterJson)) {
            List<Filter> list = jsonToList(filterJson, Filter.class);
            model.setFilter(list);
        }

        // 转换配置
        String convertJson = params.get("convert");
        if (StringUtils.isNotBlank(convertJson)) {
            List<Convert> convert = jsonToList(convertJson, Convert.class);
            model.setConvert(convert);
        }

        // 插件配置
        String pluginClassName = params.get("pluginClassName");
        Plugin plugin = null;
        if (StringUtils.isNotBlank(pluginClassName)) {
            List<Plugin> plugins = pluginService.getPluginAll();
            if (!CollectionUtils.isEmpty(plugins)) {
                for (Plugin p : plugins) {
                    if (StringUtils.equals(p.getClassName(), pluginClassName)) {
                        plugin = p;
                        break;
                    }
                }
            }
        }
        model.setPlugin(plugin);
    }

    private <T> List<T> jsonToList(String json, Class<T> valueType){
        try {
            JSONArray array = new JSONArray(json);
            if(null != array){
                List<T> list = new ArrayList<>();
                int length = array.length();
                for (int i = 0; i < length; i++) {
                    JSONObject obj = array.getJSONObject(i);
                    T t = JsonUtil.jsonToObj(obj.toString(), valueType);
                    list.add(t);
                }
                return list;
            }
        } catch (JSONException e) {
            throw new BizException(String.format("解析高级配置参数异常:%s", json));
        }
        return Collections.EMPTY_LIST;
    }

}