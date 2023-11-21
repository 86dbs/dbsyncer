package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.enums.BufferActuatorMetricEnum;
import org.dbsyncer.biz.enums.DiskMetricEnum;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.metric.MetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.CpuMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.DiskMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.DoubleRoundMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.GCMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.MemoryMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.ValueMetricDetailFormatter;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.vo.AppReportMetricVo;
import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.LogVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.biz.vo.MetricResponseVo;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.sdk.scheduled.ScheduledTaskJob;
import org.dbsyncer.sdk.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.BooleanFilter;
import org.dbsyncer.storage.query.Query;
import org.dbsyncer.storage.query.filter.LongFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
public class MonitorServiceImpl extends BaseServiceImpl implements MonitorService, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MetricReporter metricReporter;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private DataSyncService dataSyncService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private StorageService storageService;

    @Resource
    private LogService logService;

    @Resource
    private SystemConfigService systemConfigService;

    private Map<String, MetricDetailFormatter> metricDetailFormatterMap = new LinkedHashMap<>();

    @PostConstruct
    private void init() {
        metricDetailFormatterMap.putIfAbsent(BufferActuatorMetricEnum.GENERAL.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(BufferActuatorMetricEnum.STORAGE.getCode(), new ValueMetricDetailFormatter());
        metricDetailFormatterMap.putIfAbsent(BufferActuatorMetricEnum.TABLE_GROUP.getCode(), new ValueMetricDetailFormatter());
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

        // 间隔10分钟预警
        scheduledTaskService.start("0 */10 * * * ?", this);
    }

    @Override
    public List<MetaVo> getMetaAll() {
        List<MetaVo> list = profileComponent.getMetaAll()
                .stream()
                .map(m -> convertMeta2Vo(m))
                .sorted(Comparator.comparing(MetaVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
    }

    @Override
    public MetaVo getMetaVo(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
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

        Paging paging = queryData(getDefaultMetaId(id), pageNum, pageSize, error, success);
        List<Map> data = (List<Map>) paging.getData();
        List<DataVo> list = new ArrayList<>();
        for (Map row : data) {
            try {
                DataVo dataVo = convert2Vo(row, DataVo.class);
                Map binlogData = dataSyncService.getBinlogData(row, true);
                dataVo.setJson(JsonUtil.objToJson(binlogData));
                list.add(dataVo);
            } catch (Exception e) {
                logger.error(e.getLocalizedMessage(), e);
            }
        }
        paging.setData(list);
        return paging;
    }

    @Override
    public String clearData(String id) {
        Assert.hasText(id, "驱动不存在.");
        Meta meta = profileComponent.getMeta(id);
        Mapping mapping = profileComponent.getMapping(meta.getMappingId());
        String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
        LogType.MappingLog log = LogType.MappingLog.CLEAR_DATA;
        logService.log(log, "%s:%s(%s)", log.getMessage(), mapping.getName(), model);
        storageService.clear(StorageEnum.DATA, id);
        return "清空同步数据成功";
    }

    @Override
    public Paging queryLog(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String json = params.get(ConfigConstant.CONFIG_MODEL_JSON);
        Query query = new Query(pageNum, pageSize);
        if (StringUtil.isNotBlank(json)) {
            query.addFilter(ConfigConstant.CONFIG_MODEL_JSON, json, true);
        }
        query.setType(StorageEnum.LOG);
        Paging paging = storageService.query(query);
        List<Map> data = (List<Map>) paging.getData();
        paging.setData(data.stream()
                .map(m -> convert2Vo(m, LogVo.class))
                .collect(Collectors.toList()));
        return paging;
    }

    @Override
    public String clearLog() {
        storageService.clear(StorageEnum.LOG, null);
        return "清空日志成功";
    }

    @Override
    public void deleteExpiredDataAndLog() {
        deleteExpiredData();
        deleteExpiredLog();
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return profileComponent.getStorageDataStatusEnumAll();
    }

    @Override
    public List<MetricEnum> getMetricEnumAll() {
        return Arrays.asList(MetricEnum.values());
    }

    @Override
    public AppReportMetricVo queryAppReportMetric(List<MetricResponse> metrics) {
        AppReportMetric appReportMetric = metricReporter.getAppReportMetric();
        AppReportMetricVo vo = new AppReportMetricVo();
        BeanUtils.copyProperties(appReportMetric, vo);
        vo.setMetrics(getMetrics(metrics));
        return vo;
    }

    @Override
    public void run() {
        // 预警：驱动出现失败记录，发送通知消息
        List<Meta> metaAll = profileComponent.getMetaAll();
        if (CollectionUtils.isEmpty(metaAll)) {
            return;
        }

        StringBuilder content = new StringBuilder();
        metaAll.forEach(meta -> {
            // 有失败记录
            if (MetaEnum.isRunning(meta.getState()) && meta.getFail().get() > 0) {
                Mapping mapping = profileComponent.getMapping(meta.getMappingId());
                if (null != mapping) {
                    ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
                    content.append("<p>");
                    content.append(String.format("%s(%s) 失败:%s, 成功:%s", mapping.getName(), modelEnum.getName(), meta.getFail(), meta.getSuccess()));
                    if (ModelEnum.FULL == modelEnum) {
                        content.append(String.format(", 总数:%s", meta.getTotal()));
                    }
                    content.append("<p>");
                }
            }
        });

        String msg = content.toString();
        if (StringUtil.isNotBlank(msg)) {
            sendNotifyMessage("同步失败", msg);
        }
    }

    private Paging queryData(String metaId, int pageNum, int pageSize, String error, String success) {
        // 没有驱动
        if (StringUtil.isBlank(metaId)) {
            return new Paging(pageNum, pageSize);
        }
        Query query = new Query(pageNum, pageSize);
        Map<String, IndexFieldResolverEnum> fieldResolvers = new LinkedHashMap<>();
        fieldResolvers.put(ConfigConstant.BINLOG_DATA, IndexFieldResolverEnum.BINARY);
        query.setIndexFieldResolverMap(fieldResolvers);

        // 查询异常信息
        if (StringUtil.isNotBlank(error)) {
            query.addFilter(ConfigConstant.DATA_ERROR, error, true);
        }
        // 查询是否成功, 默认查询失败
        query.addFilter(ConfigConstant.DATA_SUCCESS, StringUtil.isNotBlank(success) ? NumberUtil.toInt(success) : StorageDataStatusEnum.FAIL.getValue());
        query.setMetaId(metaId);
        query.setType(StorageEnum.DATA);
        return storageService.query(query);
    }

    private void deleteExpiredData() {
        List<MetaVo> metaAll = getMetaAll();
        if (!CollectionUtils.isEmpty(metaAll)) {
            Query query = new Query();
            query.setType(StorageEnum.DATA);
            int expireDataDays = systemConfigService.getSystemConfigVo().getExpireDataDays();
            long expiredTime = Timestamp.valueOf(LocalDateTime.now().minusDays(expireDataDays)).getTime();
            LongFilter expiredFilter = new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, expiredTime);
            query.setBooleanFilter(new BooleanFilter().add(expiredFilter));
            metaAll.forEach(metaVo -> {
                query.setMetaId(metaVo.getId());
                storageService.delete(query);
            });
        }
    }

    private void deleteExpiredLog() {
        Query query = new Query();
        query.setType(StorageEnum.LOG);
        int expireLogDays = systemConfigService.getSystemConfigVo().getExpireLogDays();
        long expiredTime = Timestamp.valueOf(LocalDateTime.now().minusDays(expireLogDays)).getTime();
        LongFilter expiredFilter = new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, expiredTime);
        query.setBooleanFilter(new BooleanFilter().add(expiredFilter));
        storageService.delete(query);
    }

    private MetaVo convertMeta2Vo(Meta meta) {
        Mapping mapping = profileComponent.getMapping(meta.getMappingId());
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
        // 系统指标
        List<MetricResponse> metricList = metricReporter.getMetricInfo();
        // 线程池状态
        metrics.addAll(metricList);

        // 转换显示
        return metrics.stream().map(metric -> {
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