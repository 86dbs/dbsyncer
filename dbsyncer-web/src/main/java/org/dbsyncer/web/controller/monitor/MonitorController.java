/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.enums.DiskMetricEnum;
import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.enums.StatisticEnum;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.model.Sample;
import org.dbsyncer.biz.vo.AppReportMetricVo;
import org.dbsyncer.biz.vo.HistoryStackVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/monitor")
public class MonitorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int COUNT = 60;
    private HistoryStackVo cpu = new HistoryStackVo();
    private HistoryStackVo memory = new HistoryStackVo();

    @Resource
    private MonitorService monitorService;

    @Resource
    private DataSyncService dataSyncService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private SystemConfigService systemConfigService;

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
        model.put("pagingData", monitorService.queryData(params));
        model.put("pagingLog", monitorService.queryLog(params));
        return "monitor/monitor.html";
    }

    @GetMapping("/page/retry")
    public String page(ModelMap model, String metaId, String messageId) {
        model.put("meta", monitorService.getMetaVo(metaId));
        model.put("message", dataSyncService.getMessageVo(metaId, messageId));
        return "monitor/retry.html";
    }

    @Scheduled(fixedRate = 5000)
    public void recordHistoryStackMetric() {
        recordHistoryStackMetric(MetricEnum.CPU_USAGE, cpu, cpuHistoryStackValueFormatterImpl);
        recordHistoryStackMetric(MetricEnum.MEMORY_USED, memory, memoryHistoryStackValueFormatterImpl);
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

    @GetMapping("/queryData")
    @ResponseBody
    public RestResult queryData(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryData(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/queryLog")
    @ResponseBody
    public RestResult queryLog(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryLog(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/clearData")
    @ResponseBody
    public RestResult clearData(String id) {
        try {
            return RestResult.restSuccess(monitorService.clearData(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/clearLog")
    @ResponseBody
    public RestResult clearLog() {
        try {
            return RestResult.restSuccess(monitorService.clearLog());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @ResponseBody
    @GetMapping("/queryAppReportMetric")
    public RestResult queryAppReportMetric() {
        try {
            List<MetricResponse> list = new ArrayList<>();
            List<MetricEnum> metricEnumList = monitorService.getMetricEnumAll();
            if (!CollectionUtils.isEmpty(metricEnumList)) {
                metricEnumList.forEach(m -> list.add(getMetricResponse(m.getCode())));
            }
            list.addAll(getDiskHealth());
            AppReportMetricVo reportMetric = monitorService.queryAppReportMetric(list);
            reportMetric.setCpu(cpu);
            reportMetric.setMemory(memory);
            return RestResult.restSuccess(reportMetric);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @ResponseBody
    @GetMapping("/getRefreshIntervalSeconds")
    public RestResult getRefreshInterval() {
        try {
            return RestResult.restSuccess(systemConfigService.getSystemConfig().getRefreshIntervalSeconds());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 硬盘状态
     *
     * @return
     */
    private List<MetricResponse> getDiskHealth() {
        List<MetricResponse> list = new ArrayList<>();
        SystemHealth health = (SystemHealth) healthEndpoint.health();
        Map<String, HealthComponent> details = health.getComponents();
        Health diskSpace = (Health) details.get("diskSpace");
        Map<String, Object> diskSpaceDetails = diskSpace.getDetails();
        list.add(createDiskMetricResponse(DiskMetricEnum.THRESHOLD, diskSpaceDetails.get("threshold")));
        list.add(createDiskMetricResponse(DiskMetricEnum.FREE, diskSpaceDetails.get("free")));
        list.add(createDiskMetricResponse(DiskMetricEnum.TOTAL, diskSpaceDetails.get("total")));
        return list;
    }

    private MetricResponse createDiskMetricResponse(DiskMetricEnum metricEnum, Object value) {
        return new MetricResponse(metricEnum.getCode(), metricEnum.getGroup(), metricEnum.getMetricName(),
                Arrays.asList(new Sample(StatisticEnum.COUNT.getTagValueRepresentation(), value)));
    }

    private MetricResponse getMetricResponse(String code) {
        MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(code, null);
        MetricResponse metricResponse = new MetricResponse();
        MetricEnum metricEnum = MetricEnum.getMetric(metric.getName());
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