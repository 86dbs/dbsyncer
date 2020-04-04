package org.dbsyncer.biz.checker;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.parser.convert.Convert;
import org.dbsyncer.parser.model.AbstractConfigModel;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.beans.factory.annotation.Autowired;

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
        String convertJson = params.get("convert");
        if (StringUtils.isNotBlank(convertJson)) {
            List<Convert> convert = JsonUtil.jsonToObj(convertJson, List.class);
            model.setConvert(convert);
        }

        // 插件配置
        String pluginClassName = params.get("pluginClassName");
        Plugin plugin = null;
        if (StringUtils.isNotBlank(pluginClassName)) {
            List<Plugin> plugins = pluginService.getPluginAll();
            if(!CollectionUtils.isEmpty(plugins)){
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