package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorServiceImpl extends BaseServiceImpl implements ConnectorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Checker connectorChecker;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);

        return manager.addConnector(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return manager.editConnector(model);
    }

    @Override
    public String remove(String id) {
        List<Mapping> mappingAll = manager.getMappingAll();
        if(!CollectionUtils.isEmpty(mappingAll)){
            mappingAll.forEach(mapping -> {
                if(StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)){
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            });
        }

        Connector connector = manager.getConnector(id);
        log(LogType.ConnectorLog.DELETE, connector);

        manager.removeConnector(id);
        return "删除连接器成功!";
    }

    @Override
    public Connector getConnector(String id) {
        return StringUtil.isNotBlank(id) ? manager.getConnector(id) : null;
    }

    @Override
    public List<Connector> getConnectorAll() {
        List<Connector> list = manager.getConnectorAll()
                .stream()
                .sorted(Comparator.comparing(Connector::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public List<String> getConnectorTypeAll() {
        List<String> list = new ArrayList<>();
        manager.getConnectorEnumAll().forEach(c -> list.add(c.getType()));
        return list;
    }

}