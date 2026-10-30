/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.role;

import org.dbsyncer.biz.RoleService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.PermissionVO;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.RoleVO;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.log.OperLog;
import org.dbsyncer.web.log.OperLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 角色管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Controller
@RequestMapping("/role")
public class RoleController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private RoleService roleService;

    @RequestMapping("")
    public String index(ModelMap model) {
        requirePermission("user:view");
        model.put("roles", roleService.listRoles());
        return "role/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        requirePermission("user:edit");
        model.put("role", new RoleVO());
        model.put("createMode", true);
        model.put("permissionGroups", groupPermissions(roleService.listPermissions(null)));
        return "role/edit";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String id) {
        requirePermission("user:view");
        RoleVO role = roleService.getRole(id);
        model.put("role", role);
        model.put("createMode", false);
        model.put("permissionGroups", groupPermissions(roleService.listPermissions(id)));
        return "role/edit";
    }

    @OperLog(type = OperLogType.USER_INSERT, message = "新增角色")
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            requirePermission("user:edit");
            Map<String, String> params = getParams(request);
            params.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
            return RestResult.restSuccess(roleService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.USER_UPDATE, message = "修改角色")
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            requirePermission("user:edit");
            Map<String, String> params = getParams(request);
            params.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
            return RestResult.restSuccess(roleService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.USER_DELETE, message = "删除角色")
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request) {
        try {
            requirePermission("user:edit");
            Map<String, String> params = getParams(request);
            params.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
            return RestResult.restSuccess(roleService.remove(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private Map<String, List<PermissionVO>> groupPermissions(List<PermissionVO> permissions) {
        Map<String, List<PermissionVO>> groups = new LinkedHashMap<>();
        if (permissions == null) {
            return groups;
        }
        for (PermissionVO permission : permissions) {
            if (permission == null) {
                continue;
            }
            String key = StringUtil.isNotBlank(permission.getModuleName())
                    ? permission.getModuleName()
                    : "其他";
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(permission);
        }
        return groups;
    }
}
