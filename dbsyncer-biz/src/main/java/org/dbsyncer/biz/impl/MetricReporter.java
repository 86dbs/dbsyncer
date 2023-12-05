package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.enums.BufferActuatorMetricEnum;
import org.dbsyncer.biz.enums.StatisticEnum;
import org.dbsyncer.biz.enums.ThreadPoolMetricEnum;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.MappingReportMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.model.MetricResponseInfo;
import org.dbsyncer.biz.model.Sample;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
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
import java.util.Comparator;
import java.util.List;
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
public class MetricReporter implements ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private BufferActuator generalBufferActuator;

    @Resource
    private BufferActuator storageBufferActuator;

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private StorageService storageService;

    private volatile boolean running;

    private LocalDateTime queryTime;

    private final MappingReportMetric mappingReportMetric = new MappingReportMetric();

    private final int SHOW_BUFFER_ACTUATOR_SIZE = 6;

    @PostConstruct
    private void init() {
        scheduledTaskService.start(5000, this);
    }

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
                Meta meta = profileComponent.getMeta(metaId);
                Mapping mapping = profileComponent.getMapping(meta.getMappingId());
                group.forEach((k, bufferActuator) -> {
                    if (bufferActuator instanceof TableGroupBufferActuator) {
                        TableGroupBufferActuator actuator = bufferActuator;
                        TableGroup tableGroup = profileComponent.getTableGroup(actuator.getTableGroupId());
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
            final List<Meta> metaAll = profileComponent.getMetaAll();
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
                Paging paging = storageService.query(query);
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
        msg.append(StringUtil.FORWARD_SLASH).append(pool.getMaximumPoolSize()).append(StringUtil.SPACE);
        msg.append(ThreadPoolMetricEnum.COMPLETED.getMetricName()).append(StringUtil.COLON).append(pool.getCompletedTaskCount());
        info.setResponse(new MetricResponse(code, group, metricName, Arrays.asList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), msg.toString()))));
        return info;
    }

}