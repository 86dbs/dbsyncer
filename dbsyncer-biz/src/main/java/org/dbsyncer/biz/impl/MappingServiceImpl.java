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
public class MappingServiceImpl implements MappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Monitor monitor;

    @Autowired
    private Checker mappingChecker;

    // 驱动启停锁
    private final static Object LOCK = new Object();

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        return manager.addMapping(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            ConfigModel model = mappingChecker.checkEditConfigModel(params);
            return manager.editMapping(model);
        }
    }

    @Override
    public String remove(String id) {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());

            // 删除meta
            manager.removeMeta(mapping.getMetaId());

            // 删除tableGroup
            List<TableGroup> groupList = manager.getTableGroupAll(id);
            if (!CollectionUtils.isEmpty(groupList)) {
                groupList.forEach(t -> manager.removeTableGroup(t.getId()));
            }
            manager.removeMapping(id);
        }
        return "驱动删除成功";
    }

    @Override
    public MappingVo getMapping(String id) {
        Mapping mapping = manager.getMapping(id);
        return convertMapping2Vo(mapping);
    }

    @Override
    public List<MappingVo> getMappingAll() {
        List<MappingVo> list = manager.getMappingAll()
                .stream()
                .map(m -> convertMapping2Vo(m))
                .sorted(Comparator.comparing(MappingVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public String start(String id) {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            manager.start(mapping);
        }
        return "驱动启动成功";
    }

    @Override
    public String stop(String id) {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            if (!isRunning(mapping.getMetaId())) {
                throw new BizException("驱动已停止.");
            }
            manager.close(mapping);
        }
        return "驱动停止成功";
    }

    @Override
    public List<MetaVo> getMetaAll() {
        List<MetaVo> list = manager.getMetaAll()
                .stream()
                .map(m -> convertMeta2Vo(m))
                .sorted(Comparator.comparing(MetaVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        String model = mapping.getModel();
        Assert.notNull(mapping, "Mapping can not be null.");
        Connector s = manager.getConnector(mapping.getSourceConnectorId());
        Connector t = manager.getConnector(mapping.getTargetConnectorId());
        ConnectorVo sConn = new ConnectorVo(monitor.alive(s.getId()));
        BeanUtils.copyProperties(s, sConn);
        ConnectorVo tConn = new ConnectorVo(monitor.alive(t.getId()));
        BeanUtils.copyProperties(t, tConn);

        // 元信息
        Meta meta = manager.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVo metaVo = new MetaVo(ModelEnum.getModelEnum(model).getName());
        BeanUtils.copyProperties(meta, metaVo);

        MappingVo vo = new MappingVo(sConn, tConn, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

    private MetaVo convertMeta2Vo(Meta meta) {
        Mapping mapping = manager.getMapping(meta.getMappingId());
        Assert.notNull(mapping, "驱动不存在.");
        ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
        MetaVo metaVo = new MetaVo(modelEnum.getName());
        BeanUtils.copyProperties(meta, metaVo);
        return metaVo;
    }

    /**
     * 检查是否存在驱动
     *
     * @param mappingId
     * @return
     */
    private Mapping assertMappingExist(String mappingId) {
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");
        return mapping;
    }

    /**
     * 检查是否运行中，运行中抛出异常提示
     *
     * @param metaId
     * @return
     */
    private void assertRunning(String metaId) {
        Assert.isTrue(!isRunning(metaId), "驱动正在运行中, 请先停止.");
    }

    private boolean isRunning(String metaId) {
        Meta meta = manager.getMeta(metaId);
        if (null != meta) {
            int state = meta.getState();
            return MetaEnum.isRunning(state);
        }
        return false;
    }

}