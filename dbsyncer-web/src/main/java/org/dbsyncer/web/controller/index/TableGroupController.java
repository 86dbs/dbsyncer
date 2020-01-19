package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.*;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
@RequestMapping("/tableGroup")
public class TableGroupController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private TableGroupService tableGroupService;

    @Autowired
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

    @RequestMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(tableGroupService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/get")
    @ResponseBody
    public RestResult get(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(tableGroupService.getTableGroup(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll(@RequestParam(value = "mappingId") String mappingId) {
        try {
            return RestResult.restSuccess(tableGroupService.getTableGroupAll(mappingId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}