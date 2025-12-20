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
import org.dbsyncer.biz.vo.TpsVO;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.web.controller.BaseController;
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
    private HistoryStackValueFormatter cpuHistoryStackValueFormatterImpl;

    @Resource
    private HistoryStackValueFormatter memoryHistoryStackValueFormatterImpl;

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
        recordHistoryStackMetric(MetricEnum.CPU_USAGE, cpu, cpuHistoryStackValueFormatterImpl);
        recordHistoryStackMetric(MetricEnum.MEMORY_USED, memory, memoryHistoryStackValueFormatterImpl);
        getDiskSpace();
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
            List<MetricResponse> list = new ArrayList<>();
            List<MetricEnum> metricEnumList = monitorService.getMetricEnumAll();
            if (!CollectionUtils.isEmpty(metricEnumList)) {
                metricEnumList.forEach(m -> list.add(getMetricResponse(m.getCode())));
            }
            AppReportMetric reportMetric = monitorService.queryAppMetric(list);
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

    private void getDiskSpace() {
        SystemHealth health = (SystemHealth) healthEndpoint.health();
        Map<String, HealthComponent> details = health.getComponents();
        Health diskSpace = (Health) details.get("diskSpace");
        Map<String, Object> diskSpaceDetails = diskSpace.getDetails();
        // 已使用
        long threshold = NumberUtil.toLong(diskSpaceDetails.get("threshold").toString());
        disk.setThreshold(convertToGB(threshold));
        // 剩余量
        long free = NumberUtil.toLong(diskSpaceDetails.get("free").toString());
        disk.setFree(convertToGB(free));
        // 总容量
        long total = NumberUtil.toLong(diskSpaceDetails.get("total").toString());
        disk.setTotal(convertToGB(total));
        // 使用百分比

    }

    private long convertToGB(long value) {
        BigDecimal decimal = new BigDecimal(String.valueOf(value));
        BigDecimal bt = divide(decimal,0);
        BigDecimal mb = divide(bt,0);
        BigDecimal gb = divide(mb,2);
        return gb.longValue();
    }

    private BigDecimal divide(BigDecimal d1, int scale) {
        return d1.divide(new BigDecimal("1024"), scale, RoundingMode.HALF_UP);
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

    private void recordHistoryStackMetric(MetricEnum metricEnum, HistoryStackVo stackVo, HistoryStackValueFormatter formatter) {
        MetricResponse metricResponse = getMetricResponse(metricEnum.getCode());
        List<Sample> measurements = metricResponse.getMeasurements();
        if (!CollectionUtils.isEmpty(measurements)) {
            stackVo.addValue(formatter.formatValue(measurements.get(0).getValue()));
            stackVo.addName(DateFormatUtil.getCurrentTime());
            optimizeStackOverflow(stackVo.getName());
            optimizeStackOverflow(stackVo.getValue());
        }
    }

    private void optimizeStackOverflow(List<Object> stack) {
        if (stack.size() >= COUNT) {
            stack.remove(0);
        }
    }

}