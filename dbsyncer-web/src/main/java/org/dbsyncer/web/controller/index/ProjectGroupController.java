package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
/**
 * 项目组
 *
 * @author xinpeng.Fu
 * @date 2022/6/23 14:59
 **/
@Controller
@RequestMapping("/projectGroup")
public class ProjectGroupController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private ConnectorService connectorService;
    @Autowired
    private MappingService mappingService;
    @Autowired
    private ProjectGroupService projectGroupService;

    @GetMapping("/page/add")
    public String pageAdd(HttpServletRequest request, ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "group/addOrEdit";
    }

    @GetMapping("/page/edit")
    public String pageEdit(HttpServletRequest request, ModelMap model, String id) {
        ProjectGroup projectGroup = projectGroupService.getProjectGroup(id);
        model.put("projectGroup", projectGroup);
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        model.put("selectedConnectors", projectGroup.getConnectorIdStr());
        model.put("selectedMappings", projectGroup.getMappingIdStr());

        return "group/addOrEdit";
    }

    /**
     * 参数：
     * name(必)
     * mappingIds
     * connectorIds
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
            logger.error("add project group error:", e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数：
     * id(必)
     * name(必)
     * mappingIds
     * connectorIds
     *
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
            logger.error("edit project group error:", e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数：
     * id(必)
     *
     * @param request
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(projectGroupService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数：
     * id(必)
     *
     * @param request
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @GetMapping("/get")
    @ResponseBody
    public RestResult get(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            ProjectGroup projectGroup = projectGroupService.getProjectGroup(id);
            Assert.notNull(projectGroup, "该项目组已被删除");
            return RestResult.restSuccess(projectGroup);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 参数：
     * id(必)
     *
     * @param request
     * @return org.dbsyncer.biz.vo.RestResult
     * @author xinpeng.Fu
     * @date 2022/6/15 16:10
     **/
    @GetMapping("/getDetail")
    @ResponseBody
    public RestResult getDetail(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(projectGroupService.getProjectGroupDetail(id));
        } catch (Exception e) {
            logger.error("getDetail error:", e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(projectGroupService.getProjectGroupAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}
