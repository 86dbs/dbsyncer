/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

@Controller
@RequestMapping("/index")
public class IndexController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProjectGroupService projectGroupService;

    @Resource
    private AppConfigService appConfigService;

    @GetMapping("")
    public String index(ModelMap model, String projectGroupId) {
        try {
            ProjectGroupVo projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            model.put("connectorSize", projectGroup.getConnectorSize());
            model.put("connectors", projectGroup.getConnectors());
            model.put("mappings", projectGroup.getMappings());
            model.put("projectGroupId", projectGroupId);
            model.put("projectGroups", projectGroupService.getProjectGroupAll());
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return "connector/list.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return RestResult.restSuccess(appConfigService.getVersionInfo(authentication.getName()));
    }
}