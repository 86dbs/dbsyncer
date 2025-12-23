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
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

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

    private final SystemInfo systemInfo = new SystemInfo();
    private final CentralProcessor processor = systemInfo.getHardware().getProcessor();
    private final GlobalMemory globalMemory = systemInfo.getHardware().getMemory();
    private long[] prevTicks = processor.getSystemCpuLoadTicks();

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
        // 采集瞬时数据
        long[] ticks = processor.getSystemCpuLoadTicks();
        if (prevTicks != null) {
            long user = ticks[CentralProcessor.TickType.USER.getIndex()] -
                    prevTicks[CentralProcessor.TickType.USER.getIndex()];
            long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] -
                    prevTicks[CentralProcessor.TickType.NICE.getIndex()];
            long system = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] -
                    prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
            long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] -
                    prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
            long total = user + nice + system + idle;

            // 用户态CPU使用率（user + nice）
            BigDecimal userCpuPercent = BigDecimal.valueOf(user + nice)
                    .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2, RoundingMode.HALF_UP);
            cpu.setUserPercent(userCpuPercent);

            // 系统态CPU使用率
            BigDecimal systemCpuPercent = BigDecimal.valueOf(system)
                    .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2, RoundingMode.HALF_UP);
            cpu.setSysPercent(systemCpuPercent);

            // 总CPU使用率（非空闲时间）
            BigDecimal totalCpuPercent = BigDecimal.valueOf(total - idle)
                    .divide(BigDecimal.valueOf(total), 6, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2, RoundingMode.HALF_UP);
            cpu.setTotalPercent(totalCpuPercent);
            prevTicks = ticks;
        } else {
            cpu.setUserPercent(BigDecimal.ZERO);
            cpu.setSysPercent(BigDecimal.ZERO);
            cpu.setTotalPercent(BigDecimal.ZERO);
        }
    }

    private void collectMemory() {
        collectStackMetric(MetricEnum.MEMORY_USED, memory, memoryValueFormatter);
        // 系统 总内存
        memory.setSysTotal(gbValueFormatter.formatValue(globalMemory.getTotal()));
        // 系统 已使用
        memory.setSysUsed(gbValueFormatter.formatValue(globalMemory.getTotal() - globalMemory.getAvailable()));
        memory.setTotalPercent(formatPercent(memory.getSysUsed(), memory.getSysTotal()));
        // JVM 已使用
        memory.setJvmUsed(gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_USED)));
        // JVM 总内存
        memory.setJvmTotal(gbValueFormatter.formatValue(collectValue(MetricEnum.MEMORY_MAX)));
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

    private void optimizeStackOverflow(List<Object> stack) {
        if (stack.size() >= COUNT) {
            stack.remove(0);
        }
    }

}