package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.connector.base.ConnectorFactory;
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
@RequestMapping("/mapping")
public class MappingController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private TableGroupService tableGroupService;


    @Resource
    ConnectorFactory connectorFactory;

    /**
     * 同步任务列表页面
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        model.put("mappings", mappingService.getMappingAll());
        return "mapping/list";
    }

    /**
     * 添加同步任务页面
     */
    @GetMapping("/pageAdd")
    public String page(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "mapping/add";
    }

    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id, Integer exclude) {

        MappingVo mapping = mappingService.getMapping(id, exclude);
        model.put("mapping", mapping);
        model.put("tableGroups", tableGroupService.getTableGroupAll(id));
        model.put("targetTable", tableGroupService.getTableGroupAll(id));
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

    @PostMapping(value = "/refreshTables")
    @ResponseBody
    public RestResult refreshTables(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.refreshMappingTables(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 获取连接器信息（类型等）
     *
     * @param connectorId 连接器ID
     * @return 连接器信息
     */
    @GetMapping("/getConnectorInfo")
    @ResponseBody
    public RestResult getConnectorInfo(@RequestParam(value = "connectorId") String connectorId) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(connectorId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 获取数据库列表或Schema列表
     *
     * @param connectorId 连接器ID
     * @return 数据库或Schema列表
     */
    @GetMapping("/getDatabaseOrSchemaList")
    @ResponseBody
    public RestResult getDatabaseOrSchemaList(@RequestParam(value = "connectorId") String connectorId) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(connectorId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}