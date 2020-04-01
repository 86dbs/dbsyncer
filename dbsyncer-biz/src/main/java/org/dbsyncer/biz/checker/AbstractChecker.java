package org.dbsyncer.biz.checker;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.convert.FieldConvert;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.constant.ConfigConstant;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    /**
     * 修改基本配置
     *
     * @param configModel
     * @param params
     */
    protected void modifyConfigModel(ConfigModel configModel, Map<String, String> params) {
        // 名称
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        if (StringUtils.isNotBlank(name)) {
            configModel.setName(name);
        }
        configModel.setUpdateTime(System.currentTimeMillis());
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
            List<Filter> filter = JsonUtil.jsonToObj(filterJson, List.class);
            model.setFilter(filter);
        }

        // 转换配置
        String fieldConvertJson = params.get("fieldConvert");
        if (StringUtils.isNotBlank(fieldConvertJson)) {
            List<FieldConvert> fieldConvert = JsonUtil.jsonToObj(fieldConvertJson, List.class);
            model.setFieldConvert(fieldConvert);
        }

        // 插件配置
        String pluginJson = params.get("plugin");
        if (StringUtils.isNotBlank(pluginJson)) {
            Plugin plugin = JsonUtil.jsonToObj(pluginJson, Plugin.class);
            model.setPlugin(plugin);
        }

    }

    /**
     * 获取检查器类型
     *
     * @param type
     * @return
     */
    protected String getCheckerType(String type) {
        return toLowerCaseFirstOne(type).concat("ConfigChecker");
    }

    /**
     * 首字母转小写
     *
     * @param s
     * @return
     */
    private String toLowerCaseFirstOne(String s) {
        if (StringUtils.isBlank(s) || Character.isLowerCase(s.charAt(0))) {
            return s;
        }
        return new StringBuilder().append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
    }

}