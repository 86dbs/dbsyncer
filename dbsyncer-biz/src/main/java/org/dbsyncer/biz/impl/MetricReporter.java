/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.enums.BufferActuatorMetricEnum;
import org.dbsyncer.biz.enums.StatisticEnum;
import org.dbsyncer.biz.enums.ThreadPoolMetricEnum;
import org.dbsyncer.biz.model.*;
import org.dbsyncer.biz.vo.SyncTrendStackVO;
import org.dbsyncer.biz.vo.TpsVO;
import org.dbsyncer.common.metric.Bucket;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.BooleanFilter;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.filter.impl.IntFilter;
import org.dbsyncer.sdk.filter.impl.LongFilter;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-04-23 11:30
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

    @Resource
    private TimeRegistry timeRegistry;

    private volatile boolean running;

    private LocalDateTime queryTime;

    private final DashboardMetric dashboardMetric = new DashboardMetric();

    private final AppReportMetric report = new AppReportMetric();

    private final static int SHOW_REPORT_DAYS = 30;

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
        return list.stream().map(MetricResponseInfo::getResponse).collect(Collectors.toList());
    }

    public Paging<MetricResponse> queryActuator(String searchMetaId, String searchKey, int pageNum, int pageSize) {
        Paging<MetricResponse> paging = new Paging<>(pageNum, pageSize);
        if (!CollectionUtils.isEmpty(bufferActuatorRouter.getRouter())) {
            List<MetricResponseInfo> tableList = new ArrayList<>();
            // 默认查所有表
            if (StringUtil.isBlank(searchMetaId)) {
                bufferActuatorRouter.getRouter().forEach((metaId, group) -> getMetricResponseInfo(metaId, group, searchKey, tableList));
            } else {
                // 查指定驱动表
                Map<String, TableGroupBufferActuator> group = bufferActuatorRouter.getRouter().get(searchMetaId);
                if (group != null) {
                    getMetricResponseInfo(searchMetaId, group, searchKey, tableList);
                }
            }
            if (!CollectionUtils.isEmpty(tableList)) {
                int offset = (pageNum * pageSize) - pageSize;
                paging.setData(tableList.stream()
                        .sorted(Comparator.comparing(MetricResponseInfo::getQueueUp).reversed())
                        .map(MetricResponseInfo::getResponse)
                        .skip(offset).limit(pageSize).collect(Collectors.toList()));
                paging.setTotal(tableList.size());
            }
        }
        return paging;
    }

    private void getMetricResponseInfo(String metaId, Map<String, TableGroupBufferActuator> group, String searchKey, List<MetricResponseInfo> tableList) {
        Meta meta = profileComponent.getMeta(metaId);
        Mapping mapping = profileComponent.getMapping(meta.getMappingId());
        String tableGroupCode = BufferActuatorMetricEnum.TABLE_GROUP.getCode();
        group.forEach((k, actuator) -> {
            if (StringUtil.isNotBlank(searchKey)) {
                if (StringUtil.contains(actuator.getTableName(), searchKey)) {
                    tableList.add(collect(actuator, tableGroupCode, mapping.getName(), actuator.getTableName()));
                }
                return;
            }
            tableList.add(collect(actuator, tableGroupCode, mapping.getName(), actuator.getTableName()));
        });
    }

    public AppReportMetric getAppReportMetric() {
        queryTime = LocalDateTime.now();
        // 堆积任务(通用执行器 + 表执行器)
        report.setQueueUp(bufferActuatorRouter.getQueueSize().addAndGet(generalBufferActuator.getQueue().size()));
        report.setQueueCapacity(bufferActuatorRouter.getQueueCapacity().addAndGet(generalBufferActuator.getQueueCapacity()));
        // 持久化任务
        report.setStorageQueueUp(storageBufferActuator.getQueue().size());
        report.setStorageQueueCapacity(storageBufferActuator.getQueueCapacity());
        // 执行器TPS
        report.setTps(getOneMinBufferActuatorRate());
        return report;
    }

    public DashboardMetric getMappingReportMetric() {
        queryTime = LocalDateTime.now();
        return dashboardMetric;
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
            if (CollectionUtils.isEmpty(metaAll)) {
                dashboardMetric.reset();
                return;
            }
            dashboardMetric.setSuccess(getMappingSuccess(metaAll));
            dashboardMetric.setFail(getMappingFail(metaAll));
            dashboardMetric.setYesterdayData(getMappingYesterdayAll(metaAll));
            dashboardMetric.setInsert(getMappingInsert(metaAll));
            dashboardMetric.setUpdate(getMappingUpdate(metaAll));
            dashboardMetric.setDelete(getMappingDelete(metaAll));
            dashboardMetric.setDdl(0);
            // 获取同步趋势数据
            updateSyncTrendData(metaAll, dashboardMetric);

            AtomicLong running = new AtomicLong();
            AtomicLong fail = new AtomicLong();
            AtomicLong lastWeek = new AtomicLong();
            long lastWeekTime = Timestamp.valueOf(LocalDateTime.now().minusWeeks(1)).getTime();
            metaAll.forEach(meta -> {
                // 统计上周任务总数
                if (meta.getCreateTime() <= lastWeekTime) {
                    lastWeek.incrementAndGet();
                }
                // 统计运行中
                if (MetaEnum.isRunning(meta.getState())) {
                    running.incrementAndGet();
                }
                // 统计失败数
                if (meta.getFail().get() > 0) {
                    fail.incrementAndGet();
                }
            });
            dashboardMetric.setTotalMeta(metaAll.size());
            dashboardMetric.setLastWeekMeta(lastWeek.get());
            dashboardMetric.setRunningMeta(running.get());
            dashboardMetric.setFailMeta(fail.get());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            running = false;
        }
    }

    private void updateSyncTrendData(List<Meta> metaAll, DashboardMetric dashboardMetric) {
        SyncTrendStackVO stack = dashboardMetric.getTrend();
        LocalDateTime now = LocalDateTime.now();
        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
        String today = DateFormatUtil.timestampToString(timestamp, DateFormatUtil.MM_DD);
        // 未加载数据
        List<String> labels = stack.getLabels();
        if (CollectionUtils.isEmpty(labels)) {
            // 获取30天前的数据
            for (int i = SHOW_REPORT_DAYS - 1; i > 0; i--) {
                Timestamp time = Timestamp.valueOf(now.minusDays(i));
                labels.add(DateFormatUtil.timestampToString(time, DateFormatUtil.MM_DD));
                stack.getSuccess().add(getMappingDataCount(metaAll, time.getTime(), StorageDataStatusEnum.SUCCESS));
                stack.getFail().add(getMappingDataCount(metaAll, time.getTime(), StorageDataStatusEnum.FAIL));
            }
            // 记录今日数据
            labels.add(today);
            stack.getSuccess().add(dashboardMetric.getSuccess());
            stack.getFail().add(dashboardMetric.getFail());
            return;
        }

        // 日期发生变更
        if (!StringUtil.equals(today, labels.get(labels.size() - 1))) {
            // 移除最早的日期
            List<String> newLabels = labels.stream().skip(1).collect(Collectors.toList());
            labels.clear();
            labels.addAll(newLabels);
            // 记录今日数据
            labels.add(today);
            stack.getSuccess().add(dashboardMetric.getSuccess());
            stack.getFail().add(dashboardMetric.getFail());
            return;
        }

        // 更新今日数据
        stack.getSuccess().set(stack.getSuccess().size() - 1, dashboardMetric.getSuccess());
        stack.getFail().set(stack.getFail().size() - 1, dashboardMetric.getFail());
    }

    /**
     * 获取执行器TPS
     *
     * @return
     */
    public TpsVO getOneMinBufferActuatorRate() {
        Bucket[] buckets = timeRegistry.meter(TimeRegistry.GENERAL_BUFFER_ACTUATOR_TPS).getBucketAll();
        TpsVO vo = new TpsVO();
        Instant now = Instant.now();
        long oneMin = now.minus(1, ChronoUnit.MINUTES).toEpochMilli();
        // 只显示1分钟内
        Map<String, Long> map = new HashMap<>();
        Stream.of(buckets).filter(b -> b.getTime() >= oneMin)
                .sorted(Comparator.comparing(Bucket::getTime))
                .forEach(b -> map.put(DateFormatUtil.timestampToString(new Timestamp(b.getTime()), DateFormatUtil.HH_MM_SS), b.get())
                );
        for (int i = 0; i < buckets.length; i++) {
            long milli = now.minus(buckets.length - i, ChronoUnit.SECONDS).toEpochMilli();
            String key = DateFormatUtil.timestampToString(new Timestamp(milli), DateFormatUtil.HH_MM_SS);
            vo.addName(key);
            vo.addValue(map.getOrDefault(key, 0L));
        }
        vo.setAverage(Math.floor(map.values().stream().mapToInt(Long::intValue).average().orElse(0)));
        return vo;
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
     * 获取所有驱动数据
     *
     * @param metaAll
     * @param time
     * @param status
     * @return
     */
    private long getMappingDataCount(List<Meta> metaAll, long time, StorageDataStatusEnum status) {
        return queryMappingMetricCount(metaAll, (query) -> {
            LongFilter filter = new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, time);
            IntFilter success = new IntFilter(ConfigConstant.DATA_SUCCESS, status.getValue());
            query.setBooleanFilter(new BooleanFilter().add(filter).add(success));
        });
    }

    /**
     * 获取昨天驱动成功+失败数
     *
     * @param metaAll
     * @return
     */
    private long getMappingYesterdayAll(List<Meta> metaAll) {
        return queryMappingMetricCount(metaAll, (query) -> {
            long yesterday = Timestamp.valueOf(LocalDateTime.now().minusDays(1)).getTime();
            LongFilter filter = new LongFilter(ConfigConstant.CONFIG_MODEL_CREATE_TIME, FilterEnum.LT, yesterday);
            query.setBooleanFilter(new BooleanFilter().add(filter));
        });
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
        String msg = "堆积" + StringUtil.COLON + info.getQueueUp() +
                StringUtil.FORWARD_SLASH + bufferActuator.getQueueCapacity() + StringUtil.SPACE +
                ThreadPoolMetricEnum.CORE_SIZE.getMetricName() + StringUtil.COLON + pool.getActiveCount() +
                StringUtil.FORWARD_SLASH + pool.getMaximumPoolSize() + StringUtil.SPACE +
                ThreadPoolMetricEnum.COMPLETED.getMetricName() + StringUtil.COLON + pool.getCompletedTaskCount();
        info.setResponse(new MetricResponse(code, group, metricName, Collections.singletonList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), msg))));
        return info;
    }

}