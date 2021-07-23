package org.dbsyncer.monitor;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.StatisticEnum;
import org.dbsyncer.monitor.enums.ThreadPoolMetricEnum;
import org.dbsyncer.monitor.model.AppReportMetric;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.monitor.model.Sample;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
@Component
public class MonitorFactory implements Monitor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Executor taskExecutor;

    @Override
    @Cacheable(value = "connector", keyGenerator = "cacheKeyGenerator")
    public boolean isAlive(String id) {
        Connector connector = manager.getConnector(id);
        return null != connector ? manager.isAliveConnectorConfig(connector.getConfig()) : false;
    }

    @Override
    public Mapping getMapping(String mappingId) {
        return manager.getMapping(mappingId);
    }

    @Override
    public List<Meta> getMetaAll() {
        return manager.getMetaAll();
    }

    @Override
    public Paging queryData(String id, int pageNum, int pageSize, String error, String success) {
        // 没有驱动
        if (StringUtils.isBlank(id)) {
            return new Paging(pageNum, pageSize);
        }

        // 查询异常信息
        Query query = new Query(pageNum, pageSize);
        if (StringUtils.isNotBlank(error)) {
            query.addFilter(ConfigConstant.DATA_ERROR, error, true);
        }
        // 查询是否成功, 默认查询失败
        query.addFilter(ConfigConstant.DATA_SUCCESS, StringUtils.isNotBlank(success) ? success : StorageDataStatusEnum.FAIL.getCode(), false, true);
        return manager.queryData(query, id);
    }

    @Override
    public void clearData(String collectionId) {
        manager.clearData(collectionId);
    }

    @Override
    public Paging queryLog(int pageNum, int pageSize, String json) {
        Query query = new Query(pageNum, pageSize);
        if (StringUtils.isNotBlank(json)) {
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
    public List<MetricResponse> getThreadPoolInfo() {
        ThreadPoolTaskExecutor threadTask = (ThreadPoolTaskExecutor) taskExecutor;
        ThreadPoolExecutor pool = threadTask.getThreadPoolExecutor();

        List<MetricResponse> list = new ArrayList<>();
        list.add(createMetricResponse(ThreadPoolMetricEnum.TASK_SUBMITTED, pool.getTaskCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.QUEUE_UP, pool.getQueue().size()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.ACTIVE, pool.getActiveCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.COMPLETED, pool.getCompletedTaskCount()));
        list.add(createMetricResponse(ThreadPoolMetricEnum.REMAINING_CAPACITY, pool.getQueue().remainingCapacity()));
        return list;
    }

    @Override
    public AppReportMetric getAppReportMetric() {
        final List<Meta> metaAll = manager.getMetaAll();
        AppReportMetric report = new AppReportMetric();
        report.setSuccess(getMappingSuccess(metaAll));
        report.setFail(getMappingFail(metaAll));
        report.setInsert(getMappingInsert(metaAll));
        report.setUpdate(getMappingUpdate(metaAll));
        report.setDelete(getMappingDelete(metaAll));
        report.setTaskNumber(getTaskNumber());
        return report;
    }

    /**
     * 获取所有驱动成功数
     *
     * @param metaAll
     * @return
     */
    private long getMappingSuccess(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_SUCCESS, StorageDataStatusEnum.SUCCESS.getCode(), false, true));
    }

    /**
     * 获取所有驱动失败数
     *
     * @param metaAll
     * @return
     */
    private long getMappingFail(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> query.addFilter(ConfigConstant.DATA_SUCCESS, StorageDataStatusEnum.FAIL.getCode(), false, true));
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

    /**
     * 获取所有监听器队列待处理数
     *
     * @return
     */
    private long getTaskNumber() {
        return 0;
    }

    private MetricResponse createMetricResponse(ThreadPoolMetricEnum metricEnum, Object value) {
        return new MetricResponse(metricEnum.getCode(), metricEnum.getGroup(), metricEnum.getMetricName(), Arrays.asList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), value)));
    }

    private long queryMappingMetricCount(List<Meta> metaAll, QueryMappingOperation operation) {
        AtomicLong total = new AtomicLong(0);
        if (!CollectionUtils.isEmpty(metaAll)) {
            Query query = new Query(1, 1);
            operation.apply(query);
            metaAll.forEach(meta -> {
                Paging paging = manager.queryData(query, meta.getId());
                total.getAndAdd(paging.getTotal());
            });
        }
        return total.get();
    }

    private interface QueryMappingOperation {
        void apply(Query query);
    }

}