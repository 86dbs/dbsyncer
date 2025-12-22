package org.dbsyncer.web.controller.datasource;

import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.ConditionVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.model.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import java.util.List;

/**
 * 数据源页面控制器
 *
 *
 */
@Controller
@RequestMapping(value = "/datasource")
public class DataSourceController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;
    @Resource
    private AppConfig appConfig;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public ModelAndView index() {
        ModelAndView mv = new ModelAndView("datasource/datasource");
        mv.addObject("appConfig",appConfig);
        return mv;
    }

    /**
     * 获取连接列表
     */
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    @ResponseBody
    public RestResult listConnector(){
        try {
            return RestResult.restSuccess(connectorService.getConnectorAll());
        } catch (Exception e) {
            logger.error("获取连接列表失败", e);
            return RestResult.restFail("获取连接列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取连接详情
     */
    @RequestMapping(value = "/connector/get", method = RequestMethod.GET)
    @ResponseBody
    public RestResult getConnector(String id) {
        try {
            Connector connector = connectorService.getConnector(id);
            return RestResult.restSuccess(connector);
        } catch (Exception e) {
            logger.error("获取连接器失败", e);
            return RestResult.restFail("获取连接器失败: " + e.getMessage());
        }
    }
}
