package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.LogVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
 * @date 2020/04/27 10:20
 */
@Service
public class MonitorServiceImpl implements MonitorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private Monitor monitor;

    @Autowired
    private Manager manager;

    @Override
    public Map getThreadInfo() {
        return monitor.getThreadInfo();
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

    @Override
    public List<DataVo> queryData(String id, int pageNum, int pageSize) {
        Assert.hasText(id, "id不能为空.");
        List<DataVo> list = manager.queryData(id, pageNum, pageSize)
                .stream()
                .map(m -> convert2Vo(m, DataVo.class))
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public String clearData(String id) {
        manager.clearData(id);
        return "清空同步数据成功";
    }

    @Override
    public List<LogVo> queryLog(String type, int pageNum, int pageSize) {
        List<LogVo> list = manager.queryLog(type, pageNum, pageSize)
                .stream()
                .map(m -> convert2Vo(m, LogVo.class))
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public String clearLog(String type) {
        manager.clearLog(type);
        return "清空日志成功";
    }

    private MetaVo convertMeta2Vo(Meta meta) {
        Mapping mapping = manager.getMapping(meta.getMappingId());
        Assert.notNull(mapping, "驱动不存在.");
        ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
        MetaVo metaVo = new MetaVo(modelEnum.getName(), mapping.getName());
        metaVo.setMappingName(mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        return metaVo;
    }

    private <T> T convert2Vo(Map map, Class<T> clazz) {
        String json = JsonUtil.objToJson(map);
        return (T) JsonUtil.jsonToObj(json, clazz);
    }

}