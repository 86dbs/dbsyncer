package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
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
    private Monitor monitor;

    @Autowired
    private Checker mappingChecker;

    @Autowired
    private Checker metaChecker;

    // 驱动启停锁
    private final static Object LOCK = new Object();

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
        logger.info("检查驱动是否停止运行");
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");
        // 删除meta
        manager.removeMeta(mapping.getMetaId());

        // 删除tableGroup
        List<TableGroup> groupList = manager.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(groupList)) {
            groupList.forEach(t -> manager.removeTableGroup(t.getId()));
        }
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
        }
        return temp;
    }

    @Override
    public String start(String id) {
        Map<String, String> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, id);

        synchronized (LOCK){
            ConfigModel model = metaChecker.checkAddConfigModel(params);
            manager.addMeta(model);
        }
        return "驱动启动成功";
    }

    @Override
    public String stop(String id) {
        Mapping mapping = manager.getMapping(id);
        Assert.notNull(mapping, "驱动不存在.");

        synchronized (LOCK){
            String metaId = mapping.getMetaId();
            if (null != manager.getMeta(metaId)) {
                manager.removeMeta(metaId);
            } else {
                throw new BizException("驱动已停止.");
            }
        }
        return "驱动停止成功";
    }

    @Override
    public List<MetaVo> getMetaAll() {
        List<MetaVo> temp = new ArrayList<>();
        List<Meta> list = manager.getMetaAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(m -> temp.add(convertMeta2Vo(m)));
        }
        return temp;
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        Assert.notNull(mapping, "Mapping can not be null.");
        Connector s = manager.getConnector(mapping.getSourceConnectorId());
        Connector t = manager.getConnector(mapping.getTargetConnectorId());
        ConnectorVo sConn = new ConnectorVo(monitor.alive(s.getId()));
        BeanUtils.copyProperties(s, sConn);
        ConnectorVo tConn = new ConnectorVo(monitor.alive(t.getId()));
        BeanUtils.copyProperties(t, tConn);

        boolean isRunning = null != manager.getMeta(mapping.getMetaId());
        MappingVo vo = new MappingVo(isRunning, sConn, tConn);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

    private MetaVo convertMeta2Vo(Meta meta) {
        Mapping mapping = manager.getMapping(meta.getMappingId());
        Assert.notNull(mapping, "驱动不存在.");
        ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
        MetaEnum metaEnum = MetaEnum.getMetaEnum(meta.getState());
        MetaVo metaVo = new MetaVo(mapping.getName(), modelEnum.getMessage(), metaEnum.getMessage());
        BeanUtils.copyProperties(meta, metaVo);
        return metaVo;
    }

}