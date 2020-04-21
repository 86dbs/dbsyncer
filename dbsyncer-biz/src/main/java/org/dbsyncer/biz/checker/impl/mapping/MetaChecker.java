package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class MetaChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        String mappingId = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");

        // 驱动和元信息1对1关系
        List<Meta> metaAll = manager.getMetaAll(mappingId);
        if (!CollectionUtils.isEmpty(metaAll)) {
            Meta meta = metaAll.get(0);
            if (MetaEnum.READY.getCode() != meta.getState()) {
                throw new BizException("驱动正在运行中.");
            }
        }

        // TODO 获取驱动数据源总条数
        AtomicInteger total = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();
        Map<String, String> map = new ConcurrentHashMap<>();
        Meta meta = new Meta(mappingId, MetaEnum.READY.getCode(), total, success, fail, map);
        meta.setType(ConfigConstant.META);
        meta.setName(ConfigConstant.META);

        // 修改基本配置
        this.modifyConfigModel(meta, params);
        return meta;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        return null;
    }

}