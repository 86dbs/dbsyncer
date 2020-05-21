package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.logger.LogService;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.parser.enums.ErrorEnum;
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
public class MappingServiceImpl extends BaseServiceImpl implements MappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Monitor monitor;

    @Autowired
    private Checker mappingChecker;

    @Autowired
    private LogService logService;

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
        final String metaId = mapping.getMetaId();
        synchronized (LOCK) {
            assertRunning(metaId);

            // 清空同步记录
            Meta meta = manager.getMeta(metaId);
            meta.getFail().set(0);
            meta.getSuccess().set(0);
            manager.editMeta(meta);

            manager.start(mapping);

            logService.log(ErrorEnum.RUNNING, String.format("%s:%s", ErrorEnum.RUNNING.getMessage(), mapping.getName()));
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
            logService.log(ErrorEnum.STOPPING, String.format("%s:%s", ErrorEnum.STOPPING.getMessage(), mapping.getName()));
        }
        return "驱动停止成功";
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
        MetaVo metaVo = new MetaVo(ModelEnum.getModelEnum(model).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);

        MappingVo vo = new MappingVo(sConn, tConn, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
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

}