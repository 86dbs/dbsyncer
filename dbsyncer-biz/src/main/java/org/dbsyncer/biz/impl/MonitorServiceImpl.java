package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.LogVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;
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
    public String getDefaultMetaId(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        if (StringUtils.isNotBlank(id)) {
            return id;
        }
        return getDefaultMetaId();
    }

    @Override
    public List<DataVo> queryData(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        // 获取默认驱动元信息
        if (StringUtils.isBlank(id)) {
            id = getDefaultMetaId();
        }

        // 没有驱动
        if (StringUtils.isBlank(id)) {
            return Collections.EMPTY_LIST;
        }

        int pageNum = NumberUtils.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtils.toInt(params.get("pageSize"), 20);
        Query query = new Query(pageNum, pageSize);
        // 查询异常信息
        String error = params.get(ConfigConstant.DATA_ERROR);
        if(StringUtils.isNotBlank(error)){
            query.put(ConfigConstant.DATA_ERROR, error, true);
        }

        List<DataVo> list = manager.queryData(query, id).stream()
                .map(m -> convert2Vo(m, DataVo.class))
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public String clearData(String id) {
        Assert.hasText(id, "驱动不存在.");
        manager.clearData(id);
        return "清空同步数据成功";
    }

    @Override
    public List<LogVo> queryLog(Map<String, String> params) {
        int pageNum = NumberUtils.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtils.toInt(params.get("pageSize"), 20);
        Query query = new Query(pageNum, pageSize);
        // 查询日志内容
        String json = params.get(ConfigConstant.CONFIG_MODEL_JSON);
        if(StringUtils.isNotBlank(json)){
            query.put(ConfigConstant.CONFIG_MODEL_JSON, json, true);
        }
        List<LogVo> list = manager.queryLog(query).stream()
                .map(m -> convert2Vo(m, LogVo.class))
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public String clearLog() {
        manager.clearLog();
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

    private String getDefaultMetaId() {
        List<MetaVo> list = getMetaAll();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0).getId();
        }
        return "";
    }
}