/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.monitor;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.model.Sample;
import org.dbsyncer.biz.vo.ClusterNodeMetricVO;
import org.dbsyncer.biz.vo.CpuVO;
import org.dbsyncer.biz.vo.DiskSpaceVO;
import org.dbsyncer.biz.vo.MemoryVO;
import org.dbsyncer.biz.vo.TpsVO;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.web.controller.monitor.ValueFormatter;
import org.dbsyncer.web.controller.monitor.impl.CpuValueFormatter;
import org.dbsyncer.web.controller.monitor.impl.GBValueFormatter;
import org.dbsyncer.web.controller.monitor.impl.MemoryValueFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.SystemHealth;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 本机系统/应用指标采集，供监控页与集群节点互探复用。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
@Component
public class LocalNodeMetricProvider {

    private static final int HISTORY_COUNT = 60;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SystemInfo systemInfo = new SystemInfo();
    private final CentralProcessor processor = systemInfo.getHardware().getProcessor();
    private final GlobalMemory globalMemory = systemInfo.getHardware().getMemory();
    private final CpuVO cpu = new CpuVO();
    private final MemoryVO memory = new MemoryVO();
    private final DiskSpaceVO disk = new DiskSpaceVO();
    private long[] prevTicks = processor.getSystemCpuLoadTicks();

    @Resource
    private MonitorService monitorService;

    @Resource
    private MetricsEndpoint metricsEndpoint;

    @Resource
    private HealthEndpoint healthEndpoint;

    @Resource
    private CpuValueFormatter cpuValueFormatter;

    @Resource
    private MemoryValueFormatter memoryValueFormatter;

    @Resource
    private GBValueFormatter gbValueFormatter;

    @Resource
    private ClusterService clusterService;

    public CpuVO getCpu() {
        return cpu;
    }

    public MemoryVO getMemory() {
        return memory;
    }

    public DiskSpaceVO getDisk() {
        return disk;
    }

    @PostConstruct
    public void init() {
        refreshSystemMetrics();
    }

    /**
     * 本机指标快照（免登录互探与聚合共用）。
     *
     * @return 指标
     */
    public ClusterNodeMetricVO snapshot() {
        ClusterNodeMetricVO vo = new ClusterNodeMetricVO();
        vo.setNodeId(clusterService.getLocalNodeId());
        vo.setLocal(true);
        vo.setLeader(clusterService.isLeader());
        vo.setReachable(true);
        vo.setCpuPercent(cpu.getTotalPercent() == null ? BigDecimal.ZERO : cpu.getTotalPercent());
        vo.setMemoryUsed(memory.getSysUsed());
        vo.setMemoryTotal(memory.getSysTotal());
        vo.setDiskUsed(disk.getUsed());
        vo.setDiskTotal(disk.getTotal());
        vo.setThreadLive(readThreadLive());
        try {
            AppReportMetric app = monitorService.queryAppMetric(Stream
                    .of(MetricEnum.THREADS_LIVE, MetricEnum.THREADS_PEAK)
                    .map(m -> getMetricResponse(m.getCode()))
                    .collect(Collectors.toList()));
            if (app != null) {
                vo.setQueueUp(app.getQueueUp());
                vo.setStorageQueueUp(app.getStorageQueueUp());
                TpsVO tps = app.getTps();
                vo.setTps(tps == null ? 0D : tps.getAverage());
            }
        } catch (Exception e) {
            logger.warn("采集应用指标失败: {}", e.getMessage());
        }
        return vo;
    }

    @Scheduled(fixedRate = 5000)
    public void refreshSystemMetrics() {
        collectCpu();
        collectMemory();
        collectDiskSpace();
    }

    private long readThreadLive() {
        try {
            MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(MetricEnum.THREADS_LIVE.getCode(), null);
            if (metric == null || CollectionUtils.isEmpty(metric.getMeasurements())) {
                return 0L;
            }
            Double value = metric.getMeasurements().get(0).getValue();
            return value == null ? 0L : value.longValue();
        } catch (Exception e) {
            return 0L;
        }
    }

    private void collectCpu() {
        collectStackMetric(MetricEnum.CPU_USAGE, cpu, cpuValueFormatter);
        long[] ticks = processor.getSystemCpuLoadTicks();
        if (prevTicks != null) {
            long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
            long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
            long system = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
            long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
            long total = user + nice + system + idle;
            if (total == 0) {
                cpu.setUserPercent(BigDecimal.ZERO);
                cpu.setSysPercent(BigDecimal.ZERO);
                cpu.setTotalPercent(BigDecimal.ZERO);
            } else {
                cpu.setUserPercent(BigDecimal.valueOf(user + nice)
                        .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100))
                        .setScale(2, RoundingMode.HALF_UP));
                cpu.setSysPercent(BigDecimal.valueOf(system)
                        .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100))
                        .setScale(2, RoundingMode.HALF_UP));
                cpu.setTotalPercent(BigDecimal.valueOf(total - idle)
                        .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(100))
                        .setScale(2, RoundingMode.HALF_UP));
            }
            prevTicks = ticks;
        } else {
            cpu.setUserPercent(BigDecimal.ZERO);
            cpu.setSysPercent(BigDecimal.ZERO);
            cpu.setTotalPercent(BigDecimal.ZERO);
        }
    }

    private void collectMemory() {
        collectStackMetric(MetricEnum.MEMORY_USED, memory, memoryValueFormatter);
        memory.setSysTotal(gbValueFormatter.formatValue(globalMemory.getTotal()));
        memory.setSysUsed(gbValueFormatter.formatValue(globalMemory.getTotal() - globalMemory.getAvailable()));
        memory.setTotalPercent(formatPercent(memory.getSysUsed(), memory.getSysTotal()));
        memory.setJvmUsed(gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_USED)));
        memory.setJvmTotal(gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_MAX)));
    }

    private void collectDiskSpace() {
        try {
            SystemHealth health = (SystemHealth) healthEndpoint.health();
            Map<String, HealthComponent> details = health.getComponents();
            Health diskSpace = (Health) details.get("diskSpace");
            if (diskSpace == null) {
                return;
            }
            Map<String, Object> diskSpaceDetails = diskSpace.getDetails();
            disk.setTotal(gbValueFormatter.formatValue(diskSpaceDetails.get("total")));
            disk.setFree(gbValueFormatter.formatValue(diskSpaceDetails.get("free")));
            disk.setUsed(disk.getTotal().subtract(disk.getFree()));
            disk.setUsedPercent(formatPercent(disk.getUsed(), disk.getTotal()));
        } catch (Exception e) {
            logger.warn("采集磁盘指标失败: {}", e.getMessage());
        }
    }

    private BigDecimal formatPercent(BigDecimal used, BigDecimal total) {
        if (total == null || total.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        if (used == null) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        return used.divide(total, 4, RoundingMode.HALF_UP)
                .multiply(new BigDecimal("100"))
                .setScale(2, RoundingMode.HALF_UP);
    }

    private MetricResponse getMetricResponse(String code) {
        MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(code, null);
        if (metric == null) {
            throw new IllegalArgumentException("不支持指标=" + code);
        }
        MetricResponse metricResponse = new MetricResponse();
        MetricEnum metricEnum = MetricEnum.getMetric(metric.getName());
        if (metricEnum == null) {
            throw new IllegalArgumentException(String.format("Metric code \"%s\" does not exist.", code));
        }
        metricResponse.setCode(metricEnum.getCode());
        metricResponse.setGroup(metricEnum.getGroup());
        metricResponse.setMetricName(metricEnum.getMetricName());
        if (!CollectionUtils.isEmpty(metric.getMeasurements())) {
            List<Sample> measurements = new ArrayList<>();
            metric.getMeasurements().forEach(s ->
                    measurements.add(new Sample(s.getStatistic().getTagValueRepresentation(), s.getValue())));
            metricResponse.setMeasurements(measurements);
        }
        return metricResponse;
    }

    private void collectStackMetric(MetricEnum metricEnum, org.dbsyncer.biz.vo.HistoryStackVO stackVo,
                                    ValueFormatter<Object, Object> formatter) {
        try {
            MetricResponse metricResponse = getMetricResponse(metricEnum.getCode());
            List<Sample> measurements = metricResponse.getMeasurements();
            if (!CollectionUtils.isEmpty(measurements)) {
                stackVo.addValue(formatter.formatValue(measurements.get(0).getValue()));
                stackVo.addName(DateFormatUtil.getCurrentTime());
                optimizeStackOverflow(stackVo.getName());
                optimizeStackOverflow(stackVo.getValue());
            }
        } catch (Exception e) {
            logger.debug("采集堆栈指标失败 {}: {}", metricEnum, e.getMessage());
        }
    }

    private Object collectValue(MetricEnum metricEnum) {
        MetricResponse metricResponse = getMetricResponse(metricEnum.getCode());
        List<Sample> measurements = metricResponse.getMeasurements();
        if (!CollectionUtils.isEmpty(measurements)) {
            return measurements.get(0).getValue();
        }
        return 0;
    }

    private void optimizeStackOverflow(List<Object> stack) {
        if (stack.size() >= HISTORY_COUNT) {
            stack.remove(0);
        }
    }

    /**
     * 节点 HTTP 根地址。
     *
     * @param ip       IP
     * @param httpPort 端口
     * @return 如 http://ip:port；非法时为空
     */
    public static String buildHttpUrl(String ip, int httpPort) {
        if (StringUtil.isBlank(ip) || httpPort <= 0) {
            return StringUtil.EMPTY;
        }
        return "http://" + ip + ":" + httpPort;
    }
}
