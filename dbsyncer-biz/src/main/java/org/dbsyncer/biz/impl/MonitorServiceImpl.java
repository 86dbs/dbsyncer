package org.dbsyncer.biz.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.metric.MetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.CpuMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.DiskMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.DoubleRoundMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.GCMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.MemoryMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.ValueMetricDetailFormatter;
import org.dbsyncer.biz.vo.AppReportMetricVo;
import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.LogVo;
import org.dbsyncer.biz.vo.MessageVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.monitor.enums.DiskMetricEnum;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.TaskMetricEnum;
import org.dbsyncer.monitor.enums.ThreadPoolMetricEnum;
import org.dbsyncer.monitor.model.AppReportMetric;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private CacheService cacheService;

    private Map<String, MetricDetailFormatter> metricDetailFormatterMap = new LinkedHashMap<>();

    @PostConstruct
    private void init() {
        metricDetailFormatterMap.putIfAbsent(TaskMetricEnum.STORAGE_ACTIVE.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(TaskMetricEnum.STORAGE_REMAINING_CAPACITY.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(ThreadPoolMetricEnum.CORE_SIZE.getCode(), new ValueMetricDetailFormatter());
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
        List<MetaVo> list = monitor.getMetaAll()
                .stream()
                .map(m -> convertMeta2Vo(m))
                .sorted(Comparator.comparing(MetaVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public MetaVo getMetaVo(String metaId) {
        Meta meta = monitor.getMeta(metaId);
        Assert.notNull(meta, "The meta is null.");

        return convertMeta2Vo(meta);
    }

    @Override
    public String getDefaultMetaId(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        return getDefaultMetaId(id);
    }

    @Override
    public Paging queryData(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String error = params.get(ConfigConstant.DATA_ERROR);
        String success = params.get(ConfigConstant.DATA_SUCCESS);

        Paging paging = monitor.queryData(getDefaultMetaId(id), pageNum, pageSize, error, success);
        List<Map> data = (List<Map>) paging.getData();
        List<DataVo> list = new ArrayList<>();
        for (Map row : data) {
            try {
                DataVo dataVo = convert2Vo(row, DataVo.class);
                Map binlogData = getBinlogData(row);
                dataVo.setJson(JsonUtil.objToJson(binlogData));
                list.add(dataVo);
            } catch (Exception e) {
                logger.error(e.getLocalizedMessage());
            }
        }
        paging.setData(list);
        return paging;
    }

    @Override
    public MessageVo getMessageVo(String metaId, String messageId) {
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        MessageVo messageVo = new MessageVo();
        try {
            Map row = monitor.getData(metaId, messageId);
            Map binlogData = getBinlogData(row);
            String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
            TableGroup tableGroup = monitor.getTableGroup(tableGroupId);
            messageVo.setTableGroup(tableGroup);
            messageVo.setRow(binlogData);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return messageVo;
    }

    @Override
    public String clearData(String id) {
        Assert.hasText(id, "驱动不存在.");
        monitor.clearData(id);
        return "清空同步数据成功";
    }

    @Override
    public Paging queryLog(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String json = params.get(ConfigConstant.CONFIG_MODEL_JSON);
        Paging paging = monitor.queryLog(pageNum, pageSize, json);
        List<Map> data = (List<Map>) paging.getData();
        paging.setData(data.stream()
                .map(m -> convert2Vo(m, LogVo.class))
                .collect(Collectors.toList()));
        return paging;
    }

    @Override
    public String clearLog() {
        monitor.clearLog();
        return "清空日志成功";
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return monitor.getStorageDataStatusEnumAll();
    }

    @Override
    public List<MetricEnum> getMetricEnumAll() {
        return monitor.getMetricEnumAll();
    }

    @Override
    public AppReportMetricVo queryAppReportMetric(List<MetricResponse> metrics) {
        AppReportMetric appReportMetric = monitor.getAppReportMetric();
        AppReportMetricVo vo = new AppReportMetricVo();
        BeanUtils.copyProperties(appReportMetric, vo);
        vo.setMetrics(getMetrics(metrics));
        return vo;
    }

    private Map getBinlogData(Map row) throws InvalidProtocolBufferException {
        String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        byte[] bytes = (byte[]) row.get(ConfigConstant.BINLOG_DATA);
        BinlogMap message = BinlogMap.parseFrom(bytes);

        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(tableGroupId, TableGroup.class);

        // 2、反序列数据
        Map<String, Object> map = new HashMap<>();
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        final Map<String, Field> fieldMap = picker.getSourceFieldMap();
        message.getRowMap().forEach((k, v) -> {
            if (fieldMap.containsKey(k)) {
                Object val = BinlogMessageUtil.deserializeValue(fieldMap.get(k).getType(), v);
                // 处理二进制对象显示
                if (null != val && val instanceof byte[]) {
                    byte[] b = (byte[]) val;
                    if (b.length > 128) {
                        map.put(k, String.format("bytes[%d]", b.length));
                        return;
                    }
                    map.put(k, Arrays.toString(b));
                    return;
                }
                map.put(k, val);
            }
        });
        return map;
    }

    private MetaVo convertMeta2Vo(Meta meta) {
        Mapping mapping = monitor.getMapping(meta.getMappingId());
        Assert.notNull(mapping, "驱动不存在.");
        ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
        MetaVo metaVo = new MetaVo(modelEnum.getName(), mapping.getName());
        metaVo.setMappingName(mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        return metaVo;
    }

    private <T> T convert2Vo(Map map, Class<T> clazz) {
        return JsonUtil.jsonToObj(JsonUtil.objToJson(map), clazz);
    }

    private String getDefaultMetaId(String id) {
        if (StringUtil.isBlank(id)) {
            List<MetaVo> list = getMetaAll();
            if (!CollectionUtils.isEmpty(list)) {
                return list.get(0).getId();
            }
        }
        return id;
    }

    private List<MetricResponseVo> getMetrics(List<MetricResponse> metrics) {
        // 线程池状态
        List<MetricResponse> metricList = monitor.getMetricInfo();
        // 系统指标
        metricList.addAll(metrics);

        // 转换显示
        return metricList.stream().map(metric -> {
            MetricResponseVo vo = new MetricResponseVo();
            BeanUtils.copyProperties(metric, vo);
            MetricDetailFormatter detailFormatter = metricDetailFormatterMap.get(vo.getCode());
            if (null != detailFormatter) {
                detailFormatter.format(vo);
            }
            return vo;
        }).collect(Collectors.toList());
    }
}