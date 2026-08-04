/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.enums.BufferActuatorMetricEnum;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.metric.MetricDetailFormatter;
import org.dbsyncer.biz.metric.MetricGroupProcessor;
import org.dbsyncer.biz.metric.impl.DoubleRoundMetricDetailFormatter;
import org.dbsyncer.biz.metric.impl.ValueMetricDetailFormatter;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.DashboardMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.vo.DataVO;
import org.dbsyncer.biz.vo.LogVO;
import org.dbsyncer.biz.vo.MetaVO;
import org.dbsyncer.biz.vo.MetricResponseVO;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.model.ConnectorOfflineContent;
import org.dbsyncer.plugin.model.MappingErrorContent;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.filter.impl.LongFilter;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-04-27 10:20
 */
@Service
public class MonitorServiceImpl extends BaseServiceImpl implements MonitorService, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MetricReporter metricReporter;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private DataSyncService dataSyncService;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private StorageService storageService;

    @Resource
    private LogService logService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private MetricGroupProcessor metricGroupProcessor;

    private final Map<String, MetricDetailFormatter> metricMap = new ConcurrentHashMap<>();

    private MetricResponse systemInfo;

    private LocalDateTime delayTime = LocalDateTime.now();

    private final AtomicLong LAST_EXECUTE_TIME = new AtomicLong(System.currentTimeMillis());

    @PostConstruct
    private void init() {
        metricMap.putIfAbsent(BufferActuatorMetricEnum.GENERAL.getCode(), new ValueMetricDetailFormatter());
        metricMap.putIfAbsent(BufferActuatorMetricEnum.STORAGE.getCode(), new ValueMetricDetailFormatter());
        metricMap.putIfAbsent(MetricEnum.THREADS_LIVE.getCode(), new DoubleRoundMetricDetailFormatter());
        metricMap.putIfAbsent(MetricEnum.THREADS_PEAK.getCode(), new DoubleRoundMetricDetailFormatter());
        metricMap.putIfAbsent(MetricEnum.SYSTEM_ENV.getCode(), vo -> {
            // 操作系统
            String osName = System.getProperty("os.name");
            // 架构
            vo.setDetail(String.format("%s %s %s", osName, System.getProperty("os.arch"), System.getProperty("os.version")));
        });
        systemInfo = new MetricResponse();
        systemInfo.setCode(MetricEnum.SYSTEM_ENV.getCode());
        systemInfo.setGroup(MetricEnum.SYSTEM_ENV.getGroup());

        // 间隔10分钟预警
        scheduledTaskService.start("0 */10 * * * ?", this);
    }

    @Override
    public Paging<MetaVO> queryMeta(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 50);
        String searchKey = params.get("searchKey");
        // 按驱动任务分页，避免扫全量 Meta（含校验/迁移等非同步任务）
        Paging<Mapping> paging = taskProfile.queryTasks(Mapping.class, pageNum, pageSize, searchKey);
        Paging<MetaVO> result = new Paging<>(pageNum, pageSize);
        if (paging == null) {
            return result;
        }
        result.setTotal(paging.getTotal());
        if (CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        List<MetaVO> rows = new ArrayList<>(paging.getData().size());
        for (Mapping mapping : paging.getData()) {
            if (mapping == null || StringUtil.isBlank(mapping.getMetaId())) {
                continue;
            }
            Meta meta = metaProfile.getMeta(mapping.getMetaId());
            if (meta == null) {
                continue;
            }
            MetaVO vo = convertMeta2Vo(meta);
            if (vo != null) {
                rows.add(vo);
            }
        }
        result.setData(rows);
        return result;
    }

    @Override
    public MetaVO getMetaVo(String metaId) {
        Meta meta = metaProfile.getMeta(metaId);
        Assert.notNull(meta, "The meta is null.");
        MetaVO vo = convertMeta2Vo(meta);
        Assert.notNull(vo, String.format("驱动不存在. metaId:%s, taskId:%s", meta.getId(), meta.getTaskId()));
        return vo;
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
        String status = params.get("status");
        String tableGroupId = params.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        if (StringUtil.isBlank(tableGroupId)) {
            tableGroupId = params.get("tableGroupId");
        }

        Paging paging = queryData(getDefaultMetaId(id), pageNum, pageSize, error, status, tableGroupId);
        List<Map> data = (List<Map>) paging.getData();
        List<DataVO> list = new ArrayList<>();
        for (Map row : data) {
            try {
                // 精简分表列映射到 DataVO：TYPE→event, IS_SUCCESS→success, TARGET_TABLE→targetTableName
                row.put("event", row.get(ConfigConstant.CONFIG_MODEL_TYPE));
                row.put("success", row.get(ConfigConstant.DETAIL_IS_SUCCESS));
                row.put(ConfigConstant.DATA_TARGET_TABLE_NAME, row.get(ConfigConstant.DETAIL_TARGET_TABLE));
                DataVO dataVo = convert2Vo(row, DataVO.class);
                // 列表不解 blob，字段详情按需拉取
                dataVo.setJson(null);
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
        return clearData(id, null);
    }

    @Override
    public String clearData(String id, String tableGroupId) {
        Assert.hasText(id, "驱动不存在.");
        Meta meta = metaProfile.getMeta(id);
        Assert.notNull(meta, "驱动不存在.");
        Mapping mapping = profileComponent.getMapping(meta.getTaskId());
        Assert.notNull(mapping, "驱动不存在.");
        String shardId = metaProfile.resolveTaskDetailShardId(meta);

        if (StringUtil.isNotBlank(tableGroupId)) {
            TableGroup tableGroup = tableGroupProfile.getTableGroup(tableGroupId);
            Assert.notNull(tableGroup, "表映射不存在.");
            Assert.isTrue(StringUtil.equals(tableGroup.getTaskId(), mapping.getId()), "表映射不属于当前驱动.");
            clearTableGroupData(meta, shardId, tableGroupId);
            LogType.MappingLog log = LogType.MappingLog.CLEAR_DATA;
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            logService.log(log, "%s:%s(%s) tableGroup=%s", log.getMessage(), mapping.getName(), model, tableGroupId);
            return "清空当前表同步数据成功";
        }

        // 任务 Meta：success/fail 一并归零
        resetMetaCounters(meta);
        // 表级 Meta 删除
        List<String> groupIds = tableGroupProfile.listTableGroupIds(mapping.getId());
        if (!CollectionUtils.isEmpty(groupIds)) {
            for (String groupId : groupIds) {
                if (StringUtil.isNotBlank(groupId)) {
                    removeTableMeta(groupId);
                }
            }
        }
        LogType.MappingLog log = LogType.MappingLog.CLEAR_DATA;
        String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
        logService.log(log, "%s:%s(%s)", log.getMessage(), mapping.getName(), model);
        clearTaskDetailShards(meta);
        return "清空同步数据成功";
    }

    private void clearTableGroupData(Meta taskMeta, String shardId, String tableGroupId) {
        Meta tableMeta = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        long tableSuccess = tableMeta != null && tableMeta.getSuccess() != null ? tableMeta.getSuccess().get() : 0L;
        long tableFail = tableMeta != null && tableMeta.getFail() != null ? tableMeta.getFail().get() : 0L;
        if (tableSuccess != 0 || tableFail != 0) {
            metaProfile.incrementMeta(MetaIncrement.of(taskMeta.getId())
                    .success(-tableSuccess)
                    .fail(-tableFail));
        }
        removeTableMeta(tableGroupId);

        deleteTableGroupDetails(taskMeta, shardId, tableGroupId);
    }

    /**
     * 删除表级同步明细；兼容历史雪花主键分表。
     */
    private void deleteTableGroupDetails(Meta taskMeta, String shardId, String tableGroupId) {
        deleteTableGroupDetailsByShard(shardId, tableGroupId);
        if (taskMeta != null && StringUtil.isNotBlank(taskMeta.getId()) && !StringUtil.equals(taskMeta.getId(), shardId)) {
            deleteTableGroupDetailsByShard(taskMeta.getId(), tableGroupId);
        }
    }

    private void deleteTableGroupDetailsByShard(String shardId, String tableGroupId) {
        Query query = new Query();
        query.setType(StorageEnum.TASK_DETAIL);
        query.setMetaId(shardId);
        query.addFilter(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
        storageService.delete(query);
    }

    /**
     * 清空明细分表；兼容历史雪花主键分表。
     */
    private void clearTaskDetailShards(Meta meta) {
        if (meta == null) {
            return;
        }
        String shardId = metaProfile.resolveTaskDetailShardId(meta);
        storageService.clear(StorageEnum.TASK_DETAIL, shardId);
        if (StringUtil.isNotBlank(meta.getId()) && !StringUtil.equals(meta.getId(), shardId)) {
            storageService.clear(StorageEnum.TASK_DETAIL, meta.getId());
        }
    }

    private void resetMetaCounters(Meta meta) {
        if (meta == null) {
            return;
        }
        long success = meta.getSuccess() != null ? meta.getSuccess().get() : 0L;
        long fail = meta.getFail() != null ? meta.getFail().get() : 0L;
        if (success != 0 || fail != 0) {
            metaProfile.incrementMeta(MetaIncrement.of(meta.getId())
                    .success(-success)
                    .fail(-fail));
        }
    }

    private void removeTableMeta(String tableGroupId) {
        Meta tableMeta = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (tableMeta != null) {
            profileComponent.removeConfigModel(tableMeta.getId());
        }
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
        paging.setData(data.stream().map(m -> convert2Vo(m, LogVO.class)).collect(Collectors.toList()));
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
        deleteExpiredFileLog();
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return profileComponent.getStorageDataStatusEnumAll();
    }

    @Override
    public AppReportMetric queryAppMetric(List<MetricResponse> metrics) {
        AppReportMetric app = metricReporter.getAppReportMetric();
        metrics.add(systemInfo);
        // 系统指标
        metrics.addAll(metricReporter.getMetricInfo());
        // 合并分组显示
        app.setMetrics(metricGroupProcessor.process(metricResponseToVo(metrics)));
        return app;
    }

    @Override
    public DashboardMetric queryDashboardMetric() {
        return metricReporter.getMappingReportMetric();
    }

    @Override
    public Paging<MetricResponse> queryActuator(Map<String, String> params) {
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String metaId = params.get(ConfigConstant.CONFIG_MODEL_ID);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        return metricReporter.queryActuator(metaId, name, pageNum, pageSize);
    }

    @Override
    public void run() {
        // 预警：仅任务级 Meta，分页扫描
        MappingErrorContent content = new MappingErrorContent();
        long endTime = System.currentTimeMillis();
        metaProfile.pageScanMetas(TaskLevelEnum.TASK.getCode(), ConfigConstant.PAGE_SIZE, page -> {
            for (Meta meta : page) {
                Mapping mapping = profileComponent.getMapping(meta.getTaskId());
                if (mapping == null || !StringUtil.equals(ConfigConstant.MAPPING, mapping.getType())) {
                    continue;
                }
                long failCount = meta.getFail() != null ? meta.getFail().get() : 0L;
                if (failCount <= 0) {
                    continue;
                }
                Query query = new Query(1, 1);
                query.setType(StorageEnum.TASK_DETAIL);
                query.setMetaId(metaProfile.resolveTaskDetailShardId(meta));
                query.addFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.GT_AND_EQUAL, LAST_EXECUTE_TIME.longValue());
                query.addFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT_AND_EQUAL, endTime);
                query.setQueryTotal(true);
                query.addFilter(ConfigConstant.DETAIL_IS_SUCCESS, 0);
                Paging queryTemp = storageService.query(query);
                if (queryTemp.getTotal() > 0) {
                    writeMappingReport(meta, content);
                }
            }
        });
        //重置上一次的时间
        LAST_EXECUTE_TIME.set(endTime);

        if (!CollectionUtils.isEmpty(content.getErrorItems())) {
            content.setTitle("同步失败");
            sendNotifyMessage(content);
        }
        // 采集连接离线状态
        collectConnectorOffline();
    }

    private void collectConnectorOffline() {
        // 防止首次启动，连接器状态还未刷新，默认还是离线状态，防止误判
        if (LocalDateTime.now().minusMinutes(1).isAfter(delayTime)) {
            // 采集连接离线状态
            ConnectorOfflineContent content = new ConnectorOfflineContent();
            connectorService.getConnectorAll().forEach(connector -> {
                if (!connector.isRunning()) {
                    ConnectorOfflineContent.ErrorItem item = new ConnectorOfflineContent.ErrorItem();
                    item.setName(connector.getName());
                    item.setType(connector.getConfig().getConnectorType());
                    item.setUrl(connector.getConfig().getUrl());
                    content.addErrorItem(item);
                }
            });
            if (!CollectionUtils.isEmpty(content.getErrorItems())) {
                content.setTitle("连接离线");
                sendNotifyMessage(content);
            }
        }
        delayTime = LocalDateTime.now();
    }

    private void writeMappingReport(Meta meta, MappingErrorContent content) {
        Mapping mapping = profileComponent.getMapping(meta.getTaskId());
        if (null != mapping) {
            ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
            MappingErrorContent.ErrorItem item = new MappingErrorContent.ErrorItem();
            item.setName(mapping.getName());
            item.setModel(modelEnum);
            item.setFail(meta.getFail());
            item.setSuccess(meta.getSuccess());
            if (ModelEnum.isFull(modelEnum.getCode())) {
                item.setTotal(meta.getTotal());
            }
            content.addErrorItem(item);
        }
    }

    private Paging queryData(String metaId, int pageNum, int pageSize, String error, String status, String tableGroupId) {
        // 没有驱动
        if (StringUtil.isBlank(metaId)) {
            return new Paging(pageNum, pageSize);
        }
        Query query = new Query(pageNum, pageSize);
        // 列表不查 DATA blob，详情弹窗再按 id 拉取
        Set<String> selectFields = new HashSet<>();
        selectFields.add(ConfigConstant.CONFIG_MODEL_ID);
        selectFields.add(ConfigConstant.DATA_TABLE_GROUP_ID);
        selectFields.add(ConfigConstant.CONFIG_MODEL_TYPE);
        selectFields.add(ConfigConstant.DETAIL_TARGET_TABLE);
        selectFields.add(ConfigConstant.DETAIL_IS_SUCCESS);
        selectFields.add(ConfigConstant.DATA_ERROR);
        selectFields.add(ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        selectFields.add(ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        query.setSelectFlied(selectFields);

        // 明细分表：查询 dbsyncer_task_detail_{taskId}
        Meta meta = metaProfile.getMeta(metaId);
        query.setMetaId(meta != null ? metaProfile.resolveTaskDetailShardId(meta) : metaId);
        // 查询异常信息
        if (StringUtil.isNotBlank(error)) {
            query.addFilter(ConfigConstant.DATA_ERROR, error, true);
        }
        // 查询数据状态
        if (StringUtil.isNotBlank(status) && !StringUtil.equals("-1", status)) {
            query.addFilter(ConfigConstant.DETAIL_IS_SUCCESS, NumberUtil.toInt(status));
        }
        if (StringUtil.isNotBlank(tableGroupId)) {
            query.addFilter(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
        }
        query.setType(StorageEnum.TASK_DETAIL);
        return storageService.query(query);
    }

    private void deleteExpiredData() {
        // 明细分表：逐个任务分表按过期时间清理
        int expireDataDays = systemConfigService.getSystemConfig().getExpireDataDays();
        long expiredTime = Timestamp.valueOf(LocalDateTime.now().minusDays(expireDataDays)).getTime();
        metaProfile.pageScanMetas(TaskLevelEnum.TASK.getCode(), ConfigConstant.PAGE_SIZE, page -> {
            for (Meta meta : page) {
                Mapping mapping = profileComponent.getMapping(meta.getTaskId());
                if (mapping == null || !StringUtil.equals(ConfigConstant.MAPPING, mapping.getType())) {
                    continue;
                }
                deleteExpiredTaskDetails(meta, expiredTime);
            }
        });
    }

    private void deleteExpiredTaskDetails(Meta meta, long expiredTime) {
        String shardId = metaProfile.resolveTaskDetailShardId(meta);
        deleteExpiredTaskDetailsByShard(shardId, expiredTime);
        if (StringUtil.isNotBlank(meta.getId()) && !StringUtil.equals(meta.getId(), shardId)) {
            deleteExpiredTaskDetailsByShard(meta.getId(), expiredTime);
        }
    }

    private void deleteExpiredTaskDetailsByShard(String shardId, long expiredTime) {
        Query query = new Query();
        query.setType(StorageEnum.TASK_DETAIL);
        query.setMetaId(shardId);
        query.setBooleanFilter(new BooleanFilter().add(new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, expiredTime)));
        storageService.delete(query);
    }

    private void deleteExpiredLog() {
        Query query = new Query();
        query.setType(StorageEnum.LOG);
        int expireLogDays = systemConfigService.getSystemConfig().getExpireLogDays();
        long expiredTime = Timestamp.valueOf(LocalDateTime.now().minusDays(expireLogDays)).getTime();
        LongFilter expiredFilter = new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, expiredTime);
        query.setBooleanFilter(new BooleanFilter().add(expiredFilter));
        storageService.delete(query);
    }

    /**
     * 删除过期的日志文件（logs目录下的归档.log文件）
     */
    private void deleteExpiredFileLog() {
        String logHome = System.getProperty("LOG_HOME", "logs");
        File logDir = new File(logHome);
        if (!logDir.exists() || !logDir.isDirectory()) {
            return;
        }
        int expireFileLogDays = systemConfigService.getSystemConfig().getExpireFileLogDays();
        long expireMillis = System.currentTimeMillis() - expireFileLogDays * 24L * 60L * 60L * 1000L;
        deleteExpiredLogFile(logDir, expireFileLogDays, expireMillis);
    }

    private void deleteExpiredLogFile(File dir, int expireFileLogDays, long expireMillis) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                deleteExpiredLogFile(file, expireFileLogDays, expireMillis);
                // 如果子目录已空，删除空目录
                File[] children = file.listFiles();
                if (children != null && children.length == 0) {
                    file.delete();
                }
            } else if (file.getName().endsWith(".log") && file.lastModified() < expireMillis) {
                if (file.delete()) {
                    logger.info("删除过期{}天的日志文件: {}", expireFileLogDays, file.getAbsolutePath());
                }
            }
        }
    }

    private MetaVO convertMeta2Vo(Meta meta) {
        Mapping mapping = profileComponent.getMapping(meta.getTaskId());
        // 非同步驱动（校验/迁移等）跳过
        if (mapping == null || !StringUtil.equals(ConfigConstant.MAPPING, mapping.getType())
                || StringUtil.isBlank(mapping.getModel())) {
            return null;
        }
        ModelEnum modelEnum = ModelEnum.getModelEnum(mapping.getModel());
        MetaVO metaVo = new MetaVO(modelEnum.getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        return metaVo;
    }

    private <T> T convert2Vo(Map map, Class<T> clazz) {
        return JsonUtil.jsonToObj(JsonUtil.objToJson(map), clazz);
    }

    private String getDefaultMetaId(String id) {
        if (StringUtil.isBlank(id)) {
            Map<String, String> params = new HashMap<>();
            params.put("pageNum", "1");
            params.put("pageSize", "1");
            Paging<MetaVO> paging = queryMeta(params);
            if (paging != null && !CollectionUtils.isEmpty(paging.getData())) {
                return paging.getData().iterator().next().getId();
            }
        }
        return id;
    }

    private List<MetricResponseVO> metricResponseToVo(Collection<MetricResponse> metrics) {
        return metrics.stream().map(metric -> {
            MetricResponseVO vo = new MetricResponseVO();
            vo.setCode(metric.getCode());
            vo.setGroup(metric.getGroup());
            vo.setMetricName(metric.getMetricName());
            vo.setMeasurements(metric.getMeasurements());
            metricMap.computeIfPresent(vo.getCode(), (k, mdf) -> {
                mdf.format(vo);
                return mdf;
            });
            return vo;
        }).collect(Collectors.toList());
    }

}