package org.dbsyncer.web.controller.index;

import org.apache.commons.lang3.StringUtils;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.VersionVo;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ProjectGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.util.List;

@Controller
@RequestMapping("/index")
public class IndexController {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Autowired
    private AppConfig appConfig;
    @Autowired
    private ProjectGroupService projectGroupService;


    @GetMapping("")
    public String index(HttpServletRequest request, ModelMap model, String projectGroupId) {
        ProjectGroup projectGroup = projectGroupService.getProjectGroup(projectGroupId);
        if (StringUtils.isBlank(projectGroupId) || null == projectGroup) {
            List<Connector> connectors = connectorService.getConnectorAll();
            List<MappingVo> mappings = mappingService.getMappingAll();
            model.put("connectors", connectors);
            model.put("mappings", mappings);
            model.put("selectedGroup", "");
        } else {
            ProjectGroup projectGroupDetail = projectGroupService.getProjectGroupDetail(projectGroupId);
            model.put("connectors", projectGroupDetail.getConnectors());
            model.put("mappings", projectGroupDetail.getMappings());
            model.put("selectedGroup", projectGroupId);
        }
        model.put("groups", projectGroupService.getProjectGroupAll());
        return "index/index.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() throws UnsupportedEncodingException {
        return RestResult.restSuccess(new VersionVo(appConfig.getName(), appConfig.getCopyright()));
    }

}

