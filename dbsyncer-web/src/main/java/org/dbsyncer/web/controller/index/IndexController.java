package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.VersionVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;

@Controller
@RequestMapping("/index")
@ConfigurationProperties(prefix = "info.app")
public class IndexController {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    private String name;

    private String copyright;

    @GetMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "index/index.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() throws UnsupportedEncodingException {
        return RestResult.restSuccess(new VersionVo(name, copyright));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCopyright() {
        return copyright;
    }

    public void setCopyright(String copyright) {
        this.copyright = copyright;
    }
}