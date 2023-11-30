package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.RestResult;
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
@RequestMapping("/mapping")
public class MappingController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Autowired
    private TableGroupService tableGroupService;

    @GetMapping("/pageAdd")
    public String page(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "mapping/add";
    }

    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id) {
        model.put("mapping", mappingService.getMapping(id));
        model.put("tableGroups", tableGroupService.getTableGroupAll(id));
        initConfig(model);
        return "mapping/" + page;
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(mappingService.copy(id));
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
            return RestResult.restSuccess(mappingService.add(params));
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
            return RestResult.restSuccess(mappingService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.start(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.stop(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/get")
    @ResponseBody
    public RestResult get(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.getMapping(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess(mappingService.getMappingAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshTables")
    @ResponseBody
    public RestResult refreshTables(@RequestParam(value = "id") String id) {
        try {
            MappingVo mapping = mappingService.getMapping(id);
            mappingService.refreshTables(mapping.getSourceConnector());
            mappingService.refreshTables(mapping.getTargetConnector());
            return RestResult.restSuccess(id);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}