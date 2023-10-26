package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.VersionVo;
import org.dbsyncer.common.config.AppConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

@Controller
@RequestMapping("/index")
public class IndexController {

    @Resource
    private ProjectGroupService projectGroupService;

    @Resource
    private AppConfig appConfig;

    @GetMapping("")
    public String index(ModelMap model, String projectGroupId) {
        ProjectGroupVo projectGroup = projectGroupService.getProjectGroup(projectGroupId);
        model.put("connectorSize", projectGroup.getConnectorSize());
        model.put("connectors", projectGroup.getConnectors());
        model.put("mappings", projectGroup.getMappings());
        model.put("projectGroupId", projectGroupId);
        model.put("projectGroups", projectGroupService.getProjectGroupAll());
        return "index/index.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() {
        return RestResult.restSuccess(new VersionVo(appConfig.getName(), appConfig.getCopyright()));
    }
}