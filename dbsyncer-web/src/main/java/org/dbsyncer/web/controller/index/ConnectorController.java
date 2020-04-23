package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
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
@RequestMapping("/connector")
public class ConnectorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConnectorService connectorService;

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

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/get")
    @ResponseBody
    public RestResult get(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(connectorService.getConnectorAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}