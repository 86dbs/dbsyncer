package org.dbsyncer.web.controller;

import org.dbsyncer.biz.ConditionService;
import org.dbsyncer.biz.ConvertService;
import org.dbsyncer.biz.PluginService;
import org.springframework.ui.ModelMap;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 22:46
 */
public abstract class BaseController {

    @Resource
    private ConditionService filterService;

    @Resource
    private ConvertService convertService;

    @Resource
    private PluginService pluginService;

    /**
     * 获取请求参数
     *
     * @param request
     * @return
     */
    protected Map<String, String> getParams(HttpServletRequest request) {
        Map<String, String[]> map = request.getParameterMap();
        Map<String, String> res = new HashMap<>();
        map.forEach((k, v) -> res.put(k, v[0]));
        return res;
    }

    /**
     * 初始化: 条件/转换/插件
     * @param model
     */
    protected void initConfig(ModelMap model) throws Exception {
        model.put("condition", filterService.getCondition());
        model.put("convert", convertService.getConvertEnumAll());
        model.put("plugin", pluginService.getPluginAll());
    }

}