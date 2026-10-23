/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.GroupService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 业务分组接口。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
@Controller
@RequestMapping("/group")
public class GroupController extends BaseController {

    private static final String PERM_CONNECTOR_EDIT = "connector:edit";
    private static final String PERM_MAPPING_EDIT = "mapping:edit";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private GroupService groupService;

    @PostMapping("/list")
    @ResponseBody
    public RestResult list() {
        try {
            return RestResult.restSuccess(groupService.list());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            requireGroupManagePermission(params.get(ConfigConstant.CONFIG_MODEL_TYPE));
            return RestResult.restSuccess(groupService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            requireGroupManagePermission(params.get(ConfigConstant.CONFIG_MODEL_TYPE));
            return RestResult.restSuccess(groupService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            requireGroupManagePermission(params.get(ConfigConstant.CONFIG_MODEL_TYPE));
            String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
            return RestResult.restSuccess(groupService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/memberCount")
    @ResponseBody
    public RestResult memberCount(@RequestParam("id") String id) {
        try {
            Map<String, Object> data = new HashMap<>(2);
            data.put("id", id);
            data.put("count", groupService.countMembers(id));
            return RestResult.restSuccess(data);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/bind")
    @ResponseBody
    public RestResult bind(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String groupId = params.get(ConfigConstant.GROUP_REL_GROUP_ID);
            String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
            String resourceId = params.get(ConfigConstant.GROUP_REL_RESOURCE_ID);
            requireGroupManagePermission(type);
            groupService.bind(groupId, type, resourceId);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 批量挂组 / 移出。resourceIds 支持逗号分隔。
     */
    @PostMapping("/bindBatch")
    @ResponseBody
    public RestResult bindBatch(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String groupId = params.get(ConfigConstant.GROUP_REL_GROUP_ID);
            String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
            requireGroupManagePermission(type);
            groupService.bindBatch(groupId, type, parseResourceIds(params.get("resourceIds")));
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 批量从指定分组移出（不影响其它组归属）。
     */
    @PostMapping("/unbindBatch")
    @ResponseBody
    public RestResult unbindBatch(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String groupId = params.get(ConfigConstant.GROUP_REL_GROUP_ID);
            String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
            requireGroupManagePermission(type);
            groupService.unbindBatch(groupId, type, parseResourceIds(params.get("resourceIds")));
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/listByResource")
    @ResponseBody
    public RestResult listByResource(@RequestParam("type") String type,
                                     @RequestParam("resourceId") String resourceId) {
        try {
            return RestResult.restSuccess(groupService.listByResource(type, resourceId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private List<String> parseResourceIds(String resourceIdsRaw) {
        List<String> resourceIds = new ArrayList<>();
        if (StringUtil.isBlank(resourceIdsRaw)) {
            return resourceIds;
        }
        String[] parts = StringUtil.split(resourceIdsRaw, StringUtil.COMMA);
        if (parts == null) {
            return resourceIds;
        }
        for (String id : parts) {
            if (StringUtil.isNotBlank(id)) {
                resourceIds.add(id.trim());
            }
        }
        return resourceIds;
    }

    @PostMapping("/get")
    @ResponseBody
    public RestResult get(@RequestParam("id") String id) {
        try {
            if (StringUtil.isBlank(id)) {
                return RestResult.restFail("分组ID不能为空");
            }
            return RestResult.restSuccess(groupService.get(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 按资源类型映射编辑权限：连接器用 connector:edit，任务类用 mapping:edit。
     * 未传类型时任一编辑权即可（兼容旧调用）。
     */
    private void requireGroupManagePermission(String resourceType) {
        if (StringUtil.equals(ConfigConstant.CONNECTOR, resourceType)) {
            requirePermission(PERM_CONNECTOR_EDIT);
            return;
        }
        if (StringUtil.equals(ConfigConstant.MAPPING, resourceType)
                || StringUtil.equals(ConfigConstant.VALIDATE_SYNC, resourceType)
                || StringUtil.equals(ConfigConstant.DATABASE_SYNC, resourceType)) {
            requirePermission(PERM_MAPPING_EDIT);
            return;
        }
        requireAnyPermission(PERM_CONNECTOR_EDIT, PERM_MAPPING_EDIT);
    }
}
