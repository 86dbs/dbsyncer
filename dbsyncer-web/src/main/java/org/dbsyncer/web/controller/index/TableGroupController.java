package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.web.controller.BaseController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import java.util.Map;

@Controller
@RequestMapping("/tableGroup")
public class TableGroupController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private MappingService mappingService;

    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id) {
        TableGroup tableGroup = tableGroupService.getTableGroup(id);
        model.put("tableGroup", tableGroup);
        String mappingId = tableGroup.getMappingId();
        model.put("mapping", mappingService.getMapping(mappingId));
        initConfig(model);
        return "mapping/" + page;
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.search(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.add(params));
        } catch (SdkException e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage(), 400);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshFields")
    @ResponseBody
    public RestResult refreshFields(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(tableGroupService.refreshFields(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "mappingId") String mappingId, @RequestParam(value = "ids") String ids) {
        try {
            return RestResult.restSuccess(tableGroupService.remove(mappingId, ids));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
