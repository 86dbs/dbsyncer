/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.model.Sample;
import org.dbsyncer.biz.vo.CpuVO;
import org.dbsyncer.biz.vo.DiskSpaceVO;
import org.dbsyncer.biz.vo.HistoryStackVo;
import org.dbsyncer.biz.vo.MemoryVO;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.web.controller.BaseController;
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
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
@RequestMapping("/monitor")
public class MonitorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int COUNT = 60;
    private final CpuVO cpu = new CpuVO();
    private final MemoryVO memory = new MemoryVO();
    private final DiskSpaceVO disk = new DiskSpaceVO();

    @Resource
    private MonitorService monitorService;

    @Resource
    private DataSyncService dataSyncService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private PreloadTemplate preloadTemplate;

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

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        Map<String, String> params = getParams(request);
        model.put("metaId", monitorService.getDefaultMetaId(params));
        model.put("meta", monitorService.getMetaAll());
        model.put("storageDataStatus", monitorService.getStorageDataStatusEnumAll());
        model.put("dataStatus", NumberUtil.toInt(params.get("dataStatus"), -1));
        model.put("pagingData", monitorService.queryData(params));
        return "monitor/list.html";
    }

    @GetMapping("/page/retry")
    public String page(ModelMap model, String metaId, String messageId) {
        MetaVo metaVo = monitorService.getMetaVo(metaId);
        model.put("meta", metaVo);
        model.put("mapping", mappingService.getMapping(metaVo.getMappingId()));
        model.put("message", dataSyncService.getMessageVo(metaId, messageId));
        return "monitor/retry.html";
    }

    @Scheduled(fixedRate = 5000)
    public void recordHistoryStackMetric() {
        collectCpu();
        collectMemory();
        collectDiskSpace();
    }

    @Scheduled(fixedRate = 10000)
    public void refreshConnectorHealth() {
        if (preloadTemplate.isPreloadCompleted()) {
            connectorService.refreshHealth();
        }
    }

    @Scheduled(fixedRate = 30000)
    public void deleteExpiredDataAndLog() {
        if (preloadTemplate.isPreloadCompleted()) {
            monitorService.deleteExpiredDataAndLog();
        }
    }

    @PostMapping("/queryData")
    @ResponseBody
    public RestResult queryData(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryData(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/queryLog")
    @ResponseBody
    public RestResult queryLog(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryLog(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/sync")
    @ResponseBody
    public RestResult sync(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(dataSyncService.sync(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/clearData")
    @ResponseBody
    public RestResult clearData(String id) {
        try {
            return RestResult.restSuccess(monitorService.clearData(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/clearLog")
    @ResponseBody
    public RestResult clearLog() {
        try {
            return RestResult.restSuccess(monitorService.clearLog());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @ResponseBody
    @GetMapping("/metric")
    public RestResult metric() {
        try {
            AppReportMetric reportMetric = monitorService.queryAppMetric(Stream.of(MetricEnum.THREADS_LIVE, MetricEnum.THREADS_PEAK, MetricEnum.GC_PAUSE)
                    .map(m -> getMetricResponse(m.getCode())).collect(Collectors.toList()));
            reportMetric.setCpu(cpu);
            reportMetric.setMemory(memory);
            reportMetric.setDisk(disk);
            return RestResult.restSuccess(reportMetric);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @ResponseBody
    @GetMapping("/dashboard")
    public RestResult dashboard() {
        try {
            return RestResult.restSuccess(monitorService.queryDashboardMetric());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @ResponseBody
    @PostMapping("/queryActuator")
    public RestResult queryActuator(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryActuator(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private void collectCpu() {
        collectStackMetric(MetricEnum.CPU_USAGE, cpu, cpuValueFormatter);
        // 总体使用率
        BigDecimal totalPercent = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        if (!CollectionUtils.isEmpty(cpu.getValue())) {
            Object lastValue = cpu.getValue().get(cpu.getValue().size() - 1);
            if (lastValue instanceof Number) {
                totalPercent = new BigDecimal(String.valueOf(lastValue)).setScale(2, RoundingMode.HALF_UP);
            }
        }
        cpu.setUsedPercent(totalPercent);
        
        // 尝试通过指标获取用户态和系统态使用率
        BigDecimal userPercent = getMetricValue("system.cpu.user");
        BigDecimal systemPercent = getMetricValue("system.cpu.system");
        
        // 如果指标不可用，使用总体使用率按比例估算（用户态通常占大部分）
        if (userPercent == null && systemPercent == null) {
            // 如果两个都获取不到，按经验比例分配：用户态75%，系统态25%
            if (totalPercent.compareTo(BigDecimal.ZERO) > 0) {
                userPercent = totalPercent.multiply(new BigDecimal("0.75")).setScale(2, RoundingMode.HALF_UP);
                systemPercent = totalPercent.multiply(new BigDecimal("0.25")).setScale(2, RoundingMode.HALF_UP);
            } else {
                userPercent = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
                systemPercent = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
            }
        } else if (userPercent == null) {
            // 只有用户态不可用，使用总体减去系统态
            userPercent = totalPercent.subtract(systemPercent);
            if (userPercent.compareTo(BigDecimal.ZERO) < 0) {
                userPercent = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
            }
        } else if (systemPercent == null) {
            // 只有系统态不可用，使用总体减去用户态
            systemPercent = totalPercent.subtract(userPercent);
            if (systemPercent.compareTo(BigDecimal.ZERO) < 0) {
                systemPercent = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
            }
        }
        
        cpu.setUserPercent(userPercent);
        cpu.setSysPercent(systemPercent);
    }

    private void collectMemory() {
        collectStackMetric(MetricEnum.MEMORY_USED, memory, memoryValueFormatter);
        // JVM 内存已使用
        BigDecimal jvmUsed = gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_USED));
        // JVM 内存总使用
        BigDecimal jvmTotal = gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_MAX));
        memory.setJvmUsed(jvmUsed);
        memory.setJvmTotal(jvmTotal);
        
        // 系统内存信息（通过 OperatingSystemMXBean 获取）
        com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long totalPhysicalMemory = osBean.getTotalPhysicalMemorySize();
        long freePhysicalMemory = osBean.getFreePhysicalMemorySize();
        long usedPhysicalMemory = totalPhysicalMemory - freePhysicalMemory;

        BigDecimal sysTotal = gbValueFormatter.formatValue(totalPhysicalMemory);
        BigDecimal sysUsed = gbValueFormatter.formatValue(usedPhysicalMemory);
        memory.setSysTotal(sysTotal);
        memory.setSysUsed(sysUsed);

        // 计算总使用百分比
        memory.setUsedPercent(formatPercent(jvmUsed.add(sysUsed), sysTotal));
    }

    private void collectDiskSpace() {
        SystemHealth health = (SystemHealth) healthEndpoint.health();
        Map<String, HealthComponent> details = health.getComponents();
        Health diskSpace = (Health) details.get("diskSpace");
        Map<String, Object> diskSpaceDetails = diskSpace.getDetails();
        // 总容量
        disk.setTotal(gbValueFormatter.formatValue(diskSpaceDetails.get("total")));
        // 剩余量
        disk.setFree(gbValueFormatter.formatValue(diskSpaceDetails.get("free")));
        // 已使用
        disk.setUsed(disk.getTotal().subtract(disk.getFree()));
        // 使用百分比
        disk.setUsedPercent(formatPercent(disk.getUsed(), disk.getTotal()));
    }

    private BigDecimal formatPercent(BigDecimal used, BigDecimal total) {
        if (total == null || total.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        if (used == null) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        BigDecimal percent = used
                .divide(total, 4, RoundingMode.HALF_UP)
                .multiply(new BigDecimal("100"));
        return percent.setScale(2, RoundingMode.HALF_UP);
    }

    private MetricResponse getMetricResponse(String code) {
        MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(code, null);
        if (metric == null) {
            throw new IllegalArgumentException("不支持指标=" + code);
        }
        MetricResponse metricResponse = new MetricResponse();
        MetricEnum metricEnum = MetricEnum.getMetric(metric.getName());
        if (metricEnum == null) {
            throw new BizException(String.format("Metric code \"%s\" does not exist.", code));
        }
        metricResponse.setCode(metricEnum.getCode());
        metricResponse.setGroup(metricEnum.getGroup());
        metricResponse.setMetricName(metricEnum.getMetricName());
        if (!CollectionUtils.isEmpty(metric.getMeasurements())) {
            List<Sample> measurements = new ArrayList<>();
            metric.getMeasurements().forEach(s -> measurements.add(new Sample(s.getStatistic().getTagValueRepresentation(), s.getValue())));
            metricResponse.setMeasurements(measurements);
        }
        return metricResponse;
    }

    private void collectStackMetric(MetricEnum metricEnum, HistoryStackVo stackVo, ValueFormatter<Object, Object> formatter) {
        MetricResponse metricResponse = getMetricResponse(metricEnum.getCode());
        List<Sample> measurements = metricResponse.getMeasurements();
        if (!CollectionUtils.isEmpty(measurements)) {
            stackVo.addValue(formatter.formatValue(measurements.get(0).getValue()));
            stackVo.addName(DateFormatUtil.getCurrentTime());
            optimizeStackOverflow(stackVo.getName());
            optimizeStackOverflow(stackVo.getValue());
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

    /**
     * 安全获取指标值，如果指标不存在则返回 null
     */
    private BigDecimal getMetricValue(String metricCode) {
        try {
            MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(metricCode, null);
            if (metric != null && !CollectionUtils.isEmpty(metric.getMeasurements())) {
                Object value = metric.getMeasurements().get(0).getValue();
                if (value != null) {
                    BigDecimal result = new BigDecimal(String.valueOf(value));
                    // 如果值小于1，说明是小数形式（0-1之间），需要乘以100转换为百分比
                    if (result.compareTo(BigDecimal.ONE) < 0) {
                        result = result.multiply(new BigDecimal("100"));
                    }
                    return result.setScale(2, RoundingMode.HALF_UP);
                }
            } else {
                logger.trace("Metric {} returned null or empty measurements", metricCode);
            }
        } catch (IllegalArgumentException e) {
            // 指标不存在，这是正常的
            logger.trace("Metric {} not found: {}", metricCode, e.getMessage());
        } catch (Exception e) {
            logger.debug("Error getting metric {}: {}", metricCode, e.getMessage());
        }
        return null;
    }

    private void optimizeStackOverflow(List<Object> stack) {
        if (stack.size() >= COUNT) {
            stack.remove(0);
        }
    }

}