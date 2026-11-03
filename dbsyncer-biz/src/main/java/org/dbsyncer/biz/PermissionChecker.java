/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.common.util.StringUtil;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

/**
 * 功能权限断言（Service / Controller 共用）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Component
public class PermissionChecker {

    @Resource
    private PermissionService permissionService;

    public void requirePermission(String username, String permissionCode) {
        Assert.hasText(username, "无法获取登录用户.");
        Assert.isTrue(permissionService.hasPermission(username, permissionCode), "无权限");
    }

    /**
     * 具备任一权限码即可（如业务分组跨模块管理）。
     */
    public void requireAnyPermission(String username, String... permissionCodes) {
        Assert.hasText(username, "无法获取登录用户.");
        Assert.isTrue(permissionCodes != null && permissionCodes.length > 0, "权限码不能为空");
        for (String code : permissionCodes) {
            if (StringUtil.isNotBlank(code) && permissionService.hasPermission(username, code)) {
                return;
            }
        }
        Assert.isTrue(false, "无权限");
    }
}
