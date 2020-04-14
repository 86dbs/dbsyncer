package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class MappingServiceImpl implements MappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Checker mappingChecker;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        return manager.addMapping(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        logger.info("检查驱动是否停止运行");
        ConfigModel model = mappingChecker.checkEditConfigModel(params);
        return manager.editMapping(model);
    }

    @Override
    public boolean remove(String mappingId) {
        manager.removeMapping(mappingId);
        return true;
    }

    @Override
    public MappingVo getMapping(String id) {
        Mapping mapping = manager.getMapping(id);
        return convertMapping2Vo(mapping);
    }

    @Override
    public List<MappingVo> getMappingAll() {
        List<MappingVo> temp = new ArrayList<>();
        List<Mapping> list = manager.getMappingAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(m -> temp.add(convertMapping2Vo(m)));
            list.clear();
        }
        return temp;
    }

    @Override
    public boolean start(String id) {
        manager.start(id);
        return true;
    }

    @Override
    public boolean stop(String id) {
        manager.stop(id);
        return true;
    }

    boolean running = false;

    /**
     * 定时推送消息
     */
    @Scheduled(fixedRate = 5000)
    public void callback() {
        running = running ? false : true;
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        Assert.notNull(mapping, "Mapping can not be null.");
        Connector s = manager.getConnector(mapping.getSourceConnectorId());
        Connector t = manager.getConnector(mapping.getTargetConnectorId());
        ConnectorVo sConn = new ConnectorVo(running);
        BeanUtils.copyProperties(s, sConn);
        ConnectorVo tConn = new ConnectorVo(running);
        BeanUtils.copyProperties(t, tConn);
        MappingVo vo = new MappingVo(running, sConn, tConn);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

}