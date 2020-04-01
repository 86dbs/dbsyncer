package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.listener.config.TimingListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.constant.ModelConstant;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        Assert.hasText(name, "mapping name is empty.");
        Assert.hasText(sourceConnectorId, "mapping sourceConnectorId is empty.");
        Assert.hasText(targetConnectorId, "mapping targetConnectorId is empty.");

        Mapping mapping = new Mapping();
        mapping.setName(name);
        mapping.setSourceConnectorId(sourceConnectorId);
        mapping.setTargetConnectorId(targetConnectorId);

        // TODO 缺少默认值
        mapping.setModel(ModelConstant.FULL);
        mapping.setListener(new TimingListenerConfig());
        String json = JsonUtil.objToJson(mapping);
        return manager.addMapping(json);
    }

    @Override
    public String edit(Map<String, String> params) {
        logger.info("检查驱动是否停止运行");
        ConfigModel model = mappingChecker.checkConfigModel(params);
        return manager.editMapping(JsonUtil.objToJson(model));
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

    private MappingVo convertMapping2Vo(Mapping mapping) {
        Assert.notNull(mapping, "Mapping can not be null.");
        Connector s = manager.getConnector(mapping.getSourceConnectorId());
        Connector t = manager.getConnector(mapping.getTargetConnectorId());
        MappingVo vo = new MappingVo(false, s, t);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

}