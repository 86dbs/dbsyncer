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
        Assert.isTrue(permissionService.hasPermission(username, permissionCode), "No permission.");
    }

    public void requireAdmin(String username) {
        Assert.hasText(username, "无法获取登录用户.");
        Assert.isTrue(permissionService.isAdmin(username), "No permission.");
    }

    public boolean isAdmin(String username) {
        return StringUtil.isNotBlank(username) && permissionService.isAdmin(username);
    }
}
