package org.dbsyncer.web.controller.datasource;

import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.ConditionVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    public RestResult listConnector(@RequestParam(required = false) String searchType, 
                                      @RequestParam(required = false) String keyword){
        try {
            List<Connector> connectors = connectorService.getConnectorAll();
            
            if (StringUtils.hasText(keyword) && StringUtils.hasText(searchType)) {
                connectors = filterConnectors(connectors, searchType, keyword);
            }
            
            return RestResult.restSuccess(connectors);
        } catch (Exception e) {
            logger.error("获取连接列表失败", e);
            return RestResult.restFail("获取连接列表失败: " + e.getMessage());
        }
    }

    /**
     * 根据搜索类型和关键词过滤连接器
     * @param connectors 连接器列表
     * @param searchType 搜索类型（name/connectorType/ip）
     * @param keyword 搜索关键词
     * @return 过滤后的连接器列表
     */
    private List<Connector> filterConnectors(List<Connector> connectors, String searchType, String keyword) {
        if (connectors == null || connectors.isEmpty()) {
            return connectors;
        }
        
        String lowerKeyword = keyword.toLowerCase();
        
        List<Connector> filtered = new ArrayList<>();
        for (Connector connector : connectors) {
            boolean match = false;
            switch (searchType) {
                case "name":
                    match = connector.getName() != null && 
                           connector.getName().toLowerCase().contains(lowerKeyword);
                    break;
                case "connectorType":
                    ConnectorConfig config = connector.getConfig();
                    if (config != null && config.getConnectorType() != null) {
                        match = config.getConnectorType().toLowerCase().contains(lowerKeyword);
                    }
                    break;
                case "ip":
                    String extractedIp = extractIpFromConnector(connector);
                    match = extractedIp.toLowerCase().contains(lowerKeyword);
                    break;
                default:
                    match = true;
            }
            if (match) {
                filtered.add(connector);
            }
        }
        
        return filtered;
    }

    /**
     * 从连接器配置中提取IP地址
     * @param connector 连接器
     * @return IP地址或域名
     */
    private String extractIpFromConnector(Connector connector) {
        ConnectorConfig config = connector.getConfig();
        if (config == null) {
            return "";
        }

        // 尝试从多个可能的字段中提取连接地址
        String[] possibleFields = {"url", "host", "urlAddress"};

        for (String fieldName : possibleFields) {
            try {
                java.lang.reflect.Field field = config.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                String value = (String) field.get(config);

                if (value != null && !value.isEmpty()) {
                    // 专门处理JDBC URL格式
                    String ip = extractIpFromJdbcUrl(value);
                    if (!ip.isEmpty()) {
                        return ip;
                    }

                    // 如果是完整URL，解析获取host
                    if (value.contains("://")) {
                        try {
                            URI uri = new URI(value);
                            String host = uri.getHost();
                            if (host != null && !host.isEmpty()) {
                                return host;
                            }
                        } catch (URISyntaxException e) {
                            // 解析失败，继续尝试其他方法
                        }
                    } else {
                        // 如果直接是IP或主机名，直接返回
                        return value;
                    }
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                // 字段不存在，继续尝试下一个字段
            }
        }

        return "";
    }

    /**
     * 从JDBC URL中提取IP地址
     * @param jdbcUrl JDBC URL
     * @return IP地址或域名
     */
    private String extractIpFromJdbcUrl(String jdbcUrl) {
        // 处理MySQL JDBC URL格式: jdbc:mysql://host:port/database
        if (jdbcUrl.startsWith("jdbc:mysql://")) {
            Pattern mysqlPattern = Pattern.compile("jdbc:mysql://([^:]+):\\d+");
            Matcher matcher = mysqlPattern.matcher(jdbcUrl);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        
        // 处理SQL Server JDBC URL格式: jdbc:sqlserver://host:port;DatabaseName=database
        if (jdbcUrl.startsWith("jdbc:sqlserver://")) {
            Pattern sqlServerPattern = Pattern.compile("jdbc:sqlserver://([^:]+):\\d+");
            Matcher matcher = sqlServerPattern.matcher(jdbcUrl);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        
        // 处理通用JDBC URL格式
        Pattern generalPattern = Pattern.compile("jdbc:\\w+://([^:]+):\\d+");
        Matcher matcher = generalPattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1);
        }
        // 使用通用正则方法作为备用方案
        String ipByRegex = extractIpByRegex(jdbcUrl);
        if (StringUtils.hasText(ipByRegex)) {
            return ipByRegex;
        }
        return "";
    }

    /**
     * 使用正则表达式从URL中提取IP地址或域名
     * @param url URL字符串
     * @return IP地址或域名
     */
    private String extractIpByRegex(String url) {
        Pattern pattern = Pattern.compile("://([^:/]+)");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
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
