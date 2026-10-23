package org.dbsyncer.web.controller;

import org.dbsyncer.biz.ConditionService;
import org.dbsyncer.biz.PermissionChecker;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.ResourceAclService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.EditionInfoVO;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.sdk.spi.LicenseService;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
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
    private PluginService pluginService;

    @Resource
    private LicenseService licenseService;

    @Resource
    private PermissionChecker permissionChecker;

    @Resource
    private ResourceAclService resourceAclService;

    /**
     * 当前登录用户名。
     */
    protected String currentUsername() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Assert.notNull(authentication, "无法获取登录用户.");
        String username = authentication.getName();
        Assert.hasText(username, "无法获取登录用户.");
        return username;
    }

    /**
     * 断言当前用户具备权限码。
     */
    protected void requirePermission(String permissionCode) {
        permissionChecker.requirePermission(currentUsername(), permissionCode);
    }

    /**
     * 断言当前用户具备任一权限码。
     */
    protected void requireAnyPermission(String... permissionCodes) {
        permissionChecker.requireAnyPermission(currentUsername(), permissionCodes);
    }

    /**
     * 断言当前用户对资源具备动作权限。
     */
    protected void requireResource(String resourceType, String resourceId, String action) {
        resourceAclService.require(currentUsername(), resourceType, resourceId, action);
    }

    /**
     * 获取请求参数（附带当前登录用户名，供 Service 层 ACL 过滤）。
     */
    protected Map<String, String> getParams(HttpServletRequest request) {
        Map<String, String[]> map = request.getParameterMap();
        Map<String, String> res = new HashMap<>();
        map.forEach((k, v)->res.put(k, v[0]));
        res.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
        return res;
    }

    /**
     * 初始化: 条件/转换/插件（插件仅加载已安装列表，不含全库关联扫描）
     */
    protected void initConfig(ModelMap model) {
        model.put("condition", filterService.getCondition());
        model.put("convert", Arrays.asList(ConvertEnum.values()));
        model.put("plugin", pluginService.listPlugins());
    }

    /**
     * 初始化: 条件
     */
    protected void initConditionConfig(ModelMap model) {
        model.put("condition", filterService.getCondition());
    }

    /**
     * 读取请求体
     */
    protected String readRequestBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    protected void initEditionInfo(ModelMap model) {
        EditionInfoVO editionInfo = new EditionInfoVO();
        editionInfo.setEdition(licenseService.getEditionEnum().getCode());
        editionInfo.setEditionName(licenseService.getEditionEnum().getMessage());
        model.put("editionInfo", editionInfo);
    }
}
