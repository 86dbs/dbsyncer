package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.Connector;
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
@RequestMapping("/connector")
public class ConnectorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @GetMapping("/list")
    public String pageList(HttpServletRequest request, ModelMap model) {
        // 获取所有连接器数据
        model.put("connectors", connectorService.getConnectorAll());
        return "connector/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(HttpServletRequest request, ModelMap model) {
        model.put("connectorTypes", connectorService.getConnectorTypeAll());
        return "connector/add";
    }

    @GetMapping("/page/add{page}")
    public String page(HttpServletRequest request, ModelMap model, @PathVariable("page") String page) {
        return "connector/add" + page;
    }

    @GetMapping("/page/edit")
    public String pageEdit(HttpServletRequest request, ModelMap model, String id) {
        model.put("connector", connectorService.getConnector(id));
        return "connector/edit";
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.search(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(connectorService.copy(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping(value = "/getPosition")
    @ResponseBody
    public RestResult getPosition(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.getPosition(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 获取连接器的数据库列表
     */
    @GetMapping("/getDatabases")
    @ResponseBody
    public RestResult getDatabases(@RequestParam("connectorId") String connectorId) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }
            return RestResult.restSuccess(connector.getDataBaseName());
        } catch (Exception e) {
            logger.error("获取数据库列表失败", e);
            return RestResult.restFail("获取数据库列表失败: " + e.getMessage());
        }
    }

    @PostMapping("/test")
    @ResponseBody
    public RestResult test(@RequestParam(value = "id") String id) {
        try {
            boolean isAlive = connectorService.isAlive(id);
            if (isAlive) {
                return RestResult.restSuccess("连接测试成功");
            } else {
                return RestResult.restFail("连接测试失败");
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail("连接测试失败: " + e.getMessage());
        }
    }

}
