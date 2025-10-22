/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import com.alibaba.fastjson2.JSON;
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

/**
 * @author cdeluser
 */
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
            ProjectGroupVo projectGroup = new ProjectGroupVo();
            if ("".equals(projectGroupId)) {
                projectGroup = projectGroupService.getProjectGroupUnUsed();
            }else {
                projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            }
            model.put("connectorSize", projectGroup.getConnectorSize());
            model.put("connectors", projectGroup.getConnectors());
            model.put("mappings", projectGroup.getMappings());
            model.put("projectGroupId", projectGroupId);
            model.put("projectGroups", projectGroupService.getProjectGroupAll());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "index/index.html";
    }

    @ResponseBody
    @GetMapping("/mappingdata")
    public String mappingdata(ModelMap model, String projectGroupId) {
        try {
            ProjectGroupVo projectGroup = new ProjectGroupVo();
            if ("".equals(projectGroupId)) {
                projectGroup = projectGroupService.getProjectGroupUnUsed();
                projectGroupId = "";
            }else {
                projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            }
            model.put("connectorSize", projectGroup.getConnectorSize());
            model.put("connectors", projectGroup.getConnectors());
            model.put("mappings", projectGroup.getMappings());
            model.put("projectGroupId", projectGroupId);
            model.put("projectGroups", projectGroupService.getProjectGroupAll());
            return JSON.toJSONString(projectGroup);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return "";
        }
    }


    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return RestResult.restSuccess(appConfigService.getVersionInfo(authentication.getName()));
    }
}