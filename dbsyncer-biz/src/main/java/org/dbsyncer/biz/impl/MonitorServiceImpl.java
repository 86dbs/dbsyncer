package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.metric.MetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.*;
import org.dbsyncer.biz.vo.*;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.monitor.enums.DiskMetricEnum;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.ThreadPoolMetricEnum;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
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

    private Map<String, MetricDetailFormatter> metricDetailFormatterMap = new LinkedHashMap<>();

    @PostConstruct
    private void init() {
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.TASK_SUBMITTED.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.QUEUE_UP.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.ACTIVE.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.COMPLETED.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.REMAINING_CAPACITY.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.THREADS_LIVE.getCode(), new DoubleRoundMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.THREADS_PEAK.getCode(), new DoubleRoundMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.MEMORY_USED.getCode(), new MemoryMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.MEMORY_COMMITTED.getCode(), new MemoryMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.MEMORY_MAX.getCode(), new MemoryMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.CPU_USAGE.getCode(), new CpuMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(MetricEnum.GC_PAUSE.getCode(), new GCMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(DiskMetricEnum.THRESHOLD.getCode(), new DiskMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(DiskMetricEnum.FREE.getCode(), new DiskMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(DiskMetricEnum.TOTAL.getCode(), new DiskMetricDetailFormatter());
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
    public Paging queryData(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        // 获取默认驱动元信息
        if (StringUtils.isBlank(id)) {
            id = getDefaultMetaId();
        }

        int pageNum = NumberUtils.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtils.toInt(params.get("pageSize"), 10);
        // 没有驱动
        if (StringUtils.isBlank(id)) {
            return new Paging(pageNum, pageSize);
        }

        Query query = new Query(pageNum, pageSize);
        // 查询异常信息
        String error = params.get(ConfigConstant.DATA_ERROR);
        if (StringUtils.isNotBlank(error)) {
            query.put(ConfigConstant.DATA_ERROR, error, true);
        }
        // 查询是否成功, 默认查询失败
        String success = params.get(ConfigConstant.DATA_SUCCESS);
        query.put(ConfigConstant.DATA_SUCCESS, StringUtils.isNotBlank(success) ? success : StorageDataStatusEnum.FAIL.getCode(), false, true);

        Paging paging = manager.queryData(query, id);
        List<Map> data = (List<Map>) paging.getData();
        paging.setData(data.stream()
                .map(m -> convert2Vo(m, DataVo.class))
                .collect(Collectors.toList()));
        return paging;
    }

    @Override
    public String clearData(String id) {
        Assert.hasText(id, "驱动不存在.");
        manager.clearData(id);
        return "清空同步数据成功";
    }

    @Override
    public Paging queryLog(Map<String, String> params) {
        int pageNum = NumberUtils.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtils.toInt(params.get("pageSize"), 10);
        Query query = new Query(pageNum, pageSize);
        // 查询日志内容
        String json = params.get(ConfigConstant.CONFIG_MODEL_JSON);
        if (StringUtils.isNotBlank(json)) {
            query.put(ConfigConstant.CONFIG_MODEL_JSON, json, true);
        }
        Paging paging = manager.queryLog(query);
        List<Map> data = (List<Map>) paging.getData();
        paging.setData(data.stream()
                .map(m -> convert2Vo(m, LogVo.class))
                .collect(Collectors.toList()));
        return paging;
    }

    @Override
    public String clearLog() {
        manager.clearLog();
        return "清空日志成功";
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return manager.getStorageDataStatusEnumAll();
    }

    @Override
    public List<MetricEnum> getMetricEnumAll() {
        return monitor.getMetricEnumAll();
    }

    @Override
    public List<MetricResponseVo> queryMetric(List<MetricResponse> metrics) {
        // 线程池状态
        List<MetricResponse> metricList = monitor.getThreadPoolInfo();
        // 系统指标
        metricList.addAll(metrics);

        // 转换显示
        return metricList.stream().map(metric -> {
            MetricResponseVo vo = new MetricResponseVo();
            BeanUtils.copyProperties(metric, vo);
            MetricDetailFormatter detailFormatter = metricDetailFormatterMap.get(vo.getCode());
            if(null != detailFormatter){
                detailFormatter.format(vo);
            }
            return vo;
        }).collect(Collectors.toList());
    }

    @Override
    public AppReportMetricVo queryAppReportMetric() {
        return new AppReportMetricVo();
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
        return JsonUtil.jsonToObj(json, clazz);
    }

    private String getDefaultMetaId() {
        List<MetaVo> list = getMetaAll();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0).getId();
        }
        return "";
    }
}