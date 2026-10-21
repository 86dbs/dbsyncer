/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.acl;

import org.dbsyncer.biz.ResourceAclService;
import org.dbsyncer.biz.RoleService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.GroupProfile;
import org.dbsyncer.web.controller.BaseController;
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
import java.util.Map;

/**
 * 资源授权（侧栏「资源授权」）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Controller
@RequestMapping("/acl")
public class ResourceAclController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ResourceAclService resourceAclService;

    @Resource
    private RoleService roleService;

    @Resource
    private GroupProfile groupProfile;

    @Resource
    private UserConfigService userConfigService;

    @RequestMapping("")
    public String index(ModelMap model) {
        requirePermission("acl:view");
        model.put("acls", resourceAclService.listAll());
        return "acl/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        requirePermission("acl:edit");
        fillOptions(model);
        return "acl/edit";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String id) {
        requirePermission("acl:view");
        model.put("acl", resourceAclService.get(id));
        fillOptions(model);
        return "acl/edit";
    }

    @PostMapping("/save")
    @ResponseBody
    public RestResult save(HttpServletRequest request) {
        try {
            requirePermission("acl:edit");
            Map<String, String> params = getParams(request);
            params.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
            return RestResult.restSuccess(resourceAclService.save(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request) {
        try {
            requirePermission("acl:edit");
            Map<String, String> params = getParams(request);
            params.put(UserConfigService.CURRENT_USER_NAME, currentUsername());
            return RestResult.restSuccess(resourceAclService.remove(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private void fillOptions(ModelMap model) {
        model.put("roles", roleService.listRoles());
        model.put("groups", groupProfile.listGroups());
        model.put("users", userConfigService.getUserInfoAll(currentUsername()));
    }
}
