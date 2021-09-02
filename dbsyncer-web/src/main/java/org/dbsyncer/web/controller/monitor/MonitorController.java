package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.AppReportMetricVo;
import org.dbsyncer.biz.vo.ConfigVo;
import org.dbsyncer.biz.vo.HistoryStackVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.monitor.enums.DiskMetricEnum;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.enums.StatisticEnum;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.monitor.model.Sample;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

import javax.servlet.http.HttpServletRequest;
import java.util.*;

@Controller
@RequestMapping("/monitor")
public class MonitorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int COUNT = 24;
    private HistoryStackVo cpu = new HistoryStackVo();
    private HistoryStackVo memory = new HistoryStackVo();

    @Autowired
    private MonitorService monitorService;

    @Autowired
    private ConfigService configService;

    @Autowired
    private MetricsEndpoint metricsEndpoint;

    @Autowired
    private HealthEndpoint healthEndpoint;

    @Autowired
    private HistoryStackValueFormatter cpuHistoryStackValueFormatterImpl;

    @Autowired
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

    @Scheduled(fixedRate = 5000)
    public void recordHistoryStackMetric() {
        recordHistoryStackMetric(MetricEnum.CPU_USAGE, cpu, cpuHistoryStackValueFormatterImpl);
        recordHistoryStackMetric(MetricEnum.MEMORY_USED, memory, memoryHistoryStackValueFormatterImpl);
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
    @GetMapping("/getRefreshInterval")
    public RestResult getRefreshInterval() {
        try {
            ConfigVo config = configService.getConfig();
            return RestResult.restSuccess(config.getRefreshInterval());
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
            addHistoryStack(stackVo.getValue(), formatter.formatValue(measurements.get(0).getValue()));
            addHistoryStack(stackVo.getName(), DateFormatUtil.getCurrentTime());
        }
    }

    private void addHistoryStack(List<Object> stack, Object value) {
        if (stack.size() >= COUNT) {
            stack.remove(0);
        }
        stack.add(value);
    }

}