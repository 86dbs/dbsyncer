package org.dbsyncer.biz.checker;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.convert.FieldConvert;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.plugin.config.Plugin;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    @Override
    public void modify(Connector connector, Map<String, String> params) {
        throw new BizException("Un supported.");
    }

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        throw new BizException("Un supported.");
    }

    /**
     * 修改：过滤条件/转换配置/插件配置
     * @param model
     * @param params
     */
    protected void modifyConfigModel(AbstractConfigModel model, Map<String, String> params){
        // 过滤条件
        String filterJson = params.get("filter");
        if(StringUtils.isNotBlank(filterJson)){
            List<Filter> filter = JsonUtil.jsonToObj(filterJson, List.class);
            model.setFilter(filter);
        }

        // 转换配置
        String fieldConvertJson = params.get("fieldConvert");
        if(StringUtils.isNotBlank(fieldConvertJson)){
            List<FieldConvert> fieldConvert = JsonUtil.jsonToObj(fieldConvertJson, List.class);
            model.setFieldConvert(fieldConvert);
        }

        // 插件配置
        String pluginJson = params.get("plugin");
        if(StringUtils.isNotBlank(pluginJson)){
            Plugin plugin = JsonUtil.jsonToObj(pluginJson, Plugin.class);
            model.setPlugin(plugin);
        }

    }

}