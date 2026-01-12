/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorServiceImpl extends BaseServiceImpl implements ConnectorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, Boolean> health = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private LogService logService;

    @Resource
    private Checker connectorChecker;

    @Override
    public String add(Map<String, String> params) throws Exception {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);

        return profileComponent.addConfigModel(model);
    }

    @Override
    public String copy(String id) throws Exception {
        Connector connector = getConnector(id);
        Assert.notNull(connector, "The connector id is invalid.");

        Map params = JsonUtil.parseMap(connector.getConfig());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, connector.getName() + "(复制)");
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.COPY, model);
        profileComponent.addConfigModel(model);

        return String.format("复制成功[%s]", model.getName());
    }

    @Override
    public String edit(Map<String, String> params) throws Exception {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return profileComponent.editConfigModel(model);
    }

    @Override
    public String remove(String id) throws Exception {
        List<Mapping> mappingAll = profileComponent.getMappingAll();
        if (!CollectionUtils.isEmpty(mappingAll)) {
            mappingAll.forEach(mapping -> {
                if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            });
        }

        Connector connector = profileComponent.getConnector(id);
        if (connector != null) {
            connectorFactory.disconnect(connector.getConfig());
            log(LogType.ConnectorLog.DELETE, connector);
            profileComponent.removeConfigModel(id);
        }
        return "删除连接器成功!";
    }

    @Override
    public Connector getConnector(String id) {
        return StringUtil.isNotBlank(id) ? profileComponent.getConnector(id) : null;
    }

    @Override
    public List<Connector> getConnectorAll() {
        return profileComponent.getConnectorAll()
                .stream()
                .sorted(Comparator.comparing(Connector::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getConnectorTypeAll() {
        ArrayList<String> connectorTypes = new ArrayList<>(connectorFactory.getConnectorTypeAll());
        connectorTypes.sort(Comparator.comparing(String::toString));
        return connectorTypes;
    }

    @Override
    public void refreshHealth() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (CollectionUtils.isEmpty(list)) {
            if (!CollectionUtils.isEmpty(health)) {
                health.clear();
            }
            return;
        }

        // 更新连接器状态
        Set<String> exist = new HashSet<>();
        list.forEach(c -> {
            health.put(c.getId(), isAlive(c.getConfig()));
            exist.add(c.getId());
        });

        // 移除删除的连接器
        Set<String> remove = new HashSet<>();
        for (Map.Entry<String, Boolean> entry : health.entrySet()) {
            if (!exist.contains(entry.getKey())) {
                remove.add(entry.getKey());
            }
        }

        if (!CollectionUtils.isEmpty(remove)) {
            remove.forEach(health::remove);
        }
    }

    @Override
    public boolean isAlive(String id) {
        return health.containsKey(id) && health.get(id);
    }

    @Override
    public Object getPosition(String id) throws Exception {
        Connector connector = getConnector(id);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        return connectorFactory.getPosition(connectorInstance);
    }

    private boolean isAlive(ConnectorConfig config) {
        try {
            return connectorFactory.isAlive(config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        }
    }

    @Override
    public List<Connector> filterConnectors(List<Connector> connectors, String searchType, String keyword) {
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

}