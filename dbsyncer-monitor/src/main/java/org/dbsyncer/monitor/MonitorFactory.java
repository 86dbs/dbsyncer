package org.dbsyncer.monitor;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.puller.BufferActuatorRouter;
import org.dbsyncer.monitor.enums.BufferActuatorMetricEnum;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.StatisticEnum;
import org.dbsyncer.monitor.enums.ThreadPoolMetricEnum;
import org.dbsyncer.monitor.model.AppReportMetric;
import org.dbsyncer.monitor.model.MappingReportMetric;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.monitor.model.MetricResponseInfo;
import org.dbsyncer.monitor.model.Sample;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
@Component
public class MonitorFactory implements Monitor, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private Manager manager;

    @Resource
    private BufferActuator generalBufferActuator;

    @Resource
    private BufferActuator storageBufferActuator;

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    private volatile boolean running;

    private LocalDateTime queryTime;

    private final MappingReportMetric mappingReportMetric = new MappingReportMetric();

    private final int SHOW_BUFFER_ACTUATOR_SIZE = 6;

    @PostConstruct
    private void init() {
        scheduledTaskService.start(5000, this);
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return manager.getMapping(mappingId);
    }

    @Override
    public TableGroup getTableGroup(String tableGroupId) {
        return manager.getTableGroup(tableGroupId);
    }

    @Override
    public List<Meta> getMetaAll() {
        return manager.getMetaAll();
    }

    @Override
    public Meta getMeta(String metaId) {
        return manager.getMeta(metaId);
    }

    @Override
    public Paging queryData(String metaId, int pageNum, int pageSize, String error, String success) {
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
        return manager.queryData(query);
    }

    @Override
    public Map getData(String metaId, String messageId) {
        Query query = new Query(1, 1);
        Map<String, IndexFieldResolverEnum> fieldResolvers = new LinkedHashMap<>();
        fieldResolvers.put(ConfigConstant.BINLOG_DATA, IndexFieldResolverEnum.BINARY);
        query.setIndexFieldResolverMap(fieldResolvers);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, messageId);
        query.setMetaId(metaId);
        Paging paging = manager.queryData(query);
        if (!CollectionUtils.isEmpty(paging.getData())) {
            List<Map> data = (List<Map>) paging.getData();
            return data.get(0);
        }
        return Collections.EMPTY_MAP;
    }

    @Override
    public void removeData(String metaId, String messageId) {
        manager.removeData(metaId, messageId);
    }

    @Override
    public void clearData(String metaId) {
        manager.clearData(metaId);
    }

    @Override
    public Paging queryLog(int pageNum, int pageSize, String json) {
        Query query = new Query(pageNum, pageSize);
        if (StringUtil.isNotBlank(json)) {
            query.addFilter(ConfigConstant.CONFIG_MODEL_JSON, json, true);
        }
        return manager.queryLog(query);
    }

    @Override
    public void clearLog() {
        manager.clearLog();
    }

    @Override
    public List<StorageDataStatusEnum> getStorageDataStatusEnumAll() {
        return manager.getStorageDataStatusEnumAll();
    }

    @Override
    public List<MetricEnum> getMetricEnumAll() {
        return Arrays.asList(MetricEnum.values());
    }

    @Override
    public List<MetricResponse> getMetricInfo() {
        List<MetricResponseInfo> list = new ArrayList<>();
        BufferActuatorMetricEnum general = BufferActuatorMetricEnum.GENERAL;
        BufferActuatorMetricEnum storage = BufferActuatorMetricEnum.STORAGE;
        list.add(collect(generalBufferActuator, general.getCode(), general.getGroup(), general.getMetricName()));
        list.add(collect(storageBufferActuator, storage.getCode(), storage.getGroup(), storage.getMetricName()));
        if (!CollectionUtils.isEmpty(bufferActuatorRouter.getRouter())) {
            List<MetricResponseInfo> tableList = new ArrayList<>();
            String tableGroupCode = BufferActuatorMetricEnum.TABLE_GROUP.getCode();
            bufferActuatorRouter.getRouter().forEach((metaId, group) -> {
                Meta meta = manager.getMeta(metaId);
                Mapping mapping = manager.getMapping(meta.getMappingId());
                group.forEach((k, bufferActuator) -> {
                    if (bufferActuator instanceof TableGroupBufferActuator) {
                        TableGroupBufferActuator actuator = bufferActuator;
                        TableGroup tableGroup = manager.getTableGroup(actuator.getTableGroupId());
                        String metricName = new StringBuilder()
                                .append(tableGroup.getSourceTable().getName())
                                .append(" > ")
                                .append(tableGroup.getTargetTable().getName()).toString();
                        tableList.add(collect(bufferActuator, tableGroupCode, mapping.getName(), metricName));
                    }
                });
            });
            List<MetricResponseInfo> sortList = tableList.stream()
                    .sorted(Comparator.comparing(MetricResponseInfo::getQueueUp).reversed())
                    .collect(Collectors.toList());
            list.addAll(sortList.size() <= SHOW_BUFFER_ACTUATOR_SIZE ? sortList : sortList.subList(0, SHOW_BUFFER_ACTUATOR_SIZE));
        }
        return list.stream().map(info -> info.getResponse()).collect(Collectors.toList());
    }

    @Override
    public AppReportMetric getAppReportMetric() {
        queryTime = LocalDateTime.now();
        AppReportMetric report = new AppReportMetric();
        report.setSuccess(mappingReportMetric.getSuccess());
        report.setFail(mappingReportMetric.getFail());
        report.setInsert(mappingReportMetric.getInsert());
        report.setUpdate(mappingReportMetric.getUpdate());
        report.setDelete(mappingReportMetric.getDelete());
        // 堆积任务(通用执行器 + 表执行器)
        report.setQueueUp(bufferActuatorRouter.getQueueSize().addAndGet(generalBufferActuator.getQueue().size()));
        report.setQueueCapacity(bufferActuatorRouter.getQueueCapacity().addAndGet(generalBufferActuator.getQueueCapacity()));
        // 持久化任务
        report.setStorageQueueUp(storageBufferActuator.getQueue().size());
        report.setStorageQueueCapacity(storageBufferActuator.getQueueCapacity());
        return report;
    }

    @Override
    public void run() {
        if (running || null == queryTime) {
            return;
        }
        // 非活动时间范围(30s内)
        if (LocalDateTime.now().minusSeconds(30).isAfter(queryTime)) {
            return;
        }

        // 刷新报表
        try {
            running = true;
            final List<Meta> metaAll = manager.getMetaAll();
            mappingReportMetric.setSuccess(getMappingSuccess(metaAll));
            mappingReportMetric.setFail(getMappingFail(metaAll));
            mappingReportMetric.setInsert(getMappingInsert(metaAll));
            mappingReportMetric.setUpdate(getMappingUpdate(metaAll));
            mappingReportMetric.setDelete(getMappingDelete(metaAll));
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            running = false;
        }
    }

    /**
     * 获取所有驱动成功数
     *
     * @param metaAll
     * @return
     */
    private long getMappingSuccess(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_SUCCESS, StorageDataStatusEnum.SUCCESS.getValue()));
    }

    /**
     * 获取所有驱动失败数
     *
     * @param metaAll
     * @return
     */
    private long getMappingFail(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_SUCCESS, StorageDataStatusEnum.FAIL.getValue()));
    }

    /**
     * 获取所有驱动事件插入数
     *
     * @param metaAll
     * @return
     */
    private long getMappingInsert(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_EVENT, ConnectorConstant.OPERTION_INSERT));
    }

    /**
     * 获取所有驱动事件更新数
     *
     * @param metaAll
     * @return
     */
    private long getMappingUpdate(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_EVENT, ConnectorConstant.OPERTION_UPDATE));
    }

    /**
     * 获取所有驱动事件删除数
     *
     * @param metaAll
     * @return
     */
    private long getMappingDelete(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_EVENT, ConnectorConstant.OPERTION_DELETE));
    }

    private long queryMappingMetricCount(List<Meta> metaAll, Consumer<Query> operation) {
        AtomicLong total = new AtomicLong(0);
        if (!CollectionUtils.isEmpty(metaAll)) {
            Query query = new Query(1, 1);
            query.setQueryTotal(true);
            query.setType(StorageEnum.DATA);
            operation.accept(query);
            metaAll.forEach(meta -> {
                query.setMetaId(meta.getId());
                Paging paging = manager.queryData(query);
                total.getAndAdd(paging.getTotal());
            });
        }
        return total.get();
    }

    private MetricResponseInfo collect(BufferActuator bufferActuator, String code, String group, String metricName) {
        MetricResponseInfo info = new MetricResponseInfo();
        ThreadPoolTaskExecutor threadTask = (ThreadPoolTaskExecutor) bufferActuator.getExecutor();
        ThreadPoolExecutor pool = threadTask.getThreadPoolExecutor();
        info.setQueueUp(bufferActuator.getQueue().size());
        StringBuilder msg = new StringBuilder();
        msg.append("堆积").append(StringUtil.COLON).append(info.getQueueUp());
        msg.append(StringUtil.FORWARD_SLASH).append(bufferActuator.getQueueCapacity()).append(StringUtil.SPACE);
        msg.append(ThreadPoolMetricEnum.CORE_SIZE.getMetricName()).append(StringUtil.COLON).append(pool.getActiveCount());
        msg.append(StringUtil.FORWARD_SLASH).append(pool.getCorePoolSize()).append(StringUtil.SPACE);
        msg.append(ThreadPoolMetricEnum.COMPLETED.getMetricName()).append(StringUtil.COLON).append(pool.getCompletedTaskCount());
        info.setResponse(new MetricResponse(code, group, metricName, Arrays.asList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), msg.toString()))));
        return info;
    }

}