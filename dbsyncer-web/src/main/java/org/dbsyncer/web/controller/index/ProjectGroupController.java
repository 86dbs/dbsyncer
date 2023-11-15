package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.RestResult;
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
import java.util.Map;

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
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "group/save";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String id) {
        model.put("projectGroup", projectGroupService.getProjectGroup(id));
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess(projectGroupService.getProjectGroupAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}