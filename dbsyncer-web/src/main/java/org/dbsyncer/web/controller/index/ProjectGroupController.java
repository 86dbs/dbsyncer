package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 分组控制器
 *
 * @author xinpeng.Fu
 * @date 2022/6/23 14:59
 **/
@Controller
@RequestMapping("/projectGroup")
public class ProjectGroupController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProjectGroupService projectGroupService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        List<ProjectGroup> projectGroupAll = projectGroupService.getProjectGroupAll();
        List<String> connectorUsed = new ArrayList<>();
        List<String> mappingUsed = new ArrayList<>();
        if(!CollectionUtils.isEmpty(projectGroupAll)){
            for (ProjectGroup projectGroup : projectGroupAll){
                connectorUsed.addAll(projectGroup.getConnectorIds());
                mappingUsed.addAll(projectGroup.getMappingIds());
            }
        }
        List<Connector> connectorAll = connectorService.getConnectorAll();
        List<MappingVo> mappingAll = mappingService.getMappingAll();
        //移除之前使用的连接、驱动
        connectorAll = connectorAll.stream().filter(connector -> !connectorUsed.contains(connector.getId())).collect(Collectors.toList());
        mappingAll = mappingAll.stream().filter(mapping -> !mappingUsed.contains(mapping.getId())).collect(Collectors.toList());
        model.put("connectors",connectorAll );
        model.put("mappings", mappingAll);
        model.put("title", "添加分组");
        return "group/save";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String id) {
        ProjectGroupVo projectGroupEdit = projectGroupService.getProjectGroup(id);
        List<ProjectGroup> projectGroupAll = projectGroupService.getProjectGroupAll();
        List<String> connectorUsed = new ArrayList<>();
        List<String> mappingUsed = new ArrayList<>();
        if(!CollectionUtils.isEmpty(projectGroupAll)){
            for (ProjectGroup projectGroup : projectGroupAll){
                connectorUsed.addAll(projectGroup.getConnectorIds());
                mappingUsed.addAll(projectGroup.getMappingIds());
            }
        }
        List<Connector> connectorAll = connectorService.getConnectorAll();
        List<MappingVo> mappingAll = mappingService.getMappingAll();
        //移除之前使用的连接、驱动
        connectorAll = connectorAll.stream().filter(connector -> !connectorUsed.contains(connector.getId())).collect(Collectors.toList());
        connectorAll.addAll(projectGroupEdit.getConnectors());
        mappingAll = mappingAll.stream().filter(mapping -> !mappingUsed.contains(mapping.getId())).collect(Collectors.toList());
        mappingAll.addAll(projectGroupEdit.getMappings());
        model.put("projectGroup", projectGroupEdit);
        model.put("connectors", connectorAll);
        model.put("mappings", mappingAll);
        model.put("title", "修改分组");
        return "group/save";
    }

    /**
     * 参数： name(必) mappingIds connectorIds
     *
     * @param request
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(projectGroupService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数： id(必) name(必) mappingIds connectorIds
     *
     * @param request
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(projectGroupService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数： id(必)
     *
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(projectGroupService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}