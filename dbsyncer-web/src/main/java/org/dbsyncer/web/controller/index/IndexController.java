package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.VersionVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/index")
public class IndexController {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Value(value = "${info.app.name}")
    private String appName;

    @Value(value = "${info.app.copyright}")
    private String appCopyRight;

    @GetMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "index/index.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() {
        return RestResult.restSuccess(new VersionVo(appName, appCopyRight));
    }

}