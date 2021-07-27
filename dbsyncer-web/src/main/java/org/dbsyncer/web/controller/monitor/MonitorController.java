package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
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
import org.springframework.boot.actuate.health.HealthEndpoint;
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

    @Autowired
    private MonitorService monitorService;

    @Autowired
    private MetricsEndpoint metricsEndpoint;

    @Autowired
    private HealthEndpoint healthEndpoint;

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

    private final static int COUNT = 3;
    private Deque<Double> cpu = new ArrayDeque<>(COUNT);
    private Deque<Double> memory = new ArrayDeque<>(COUNT);

    @Scheduled(fixedRate = 5000)
    public void recordHistoryStackMertic() {
        // TODO 统计最近记录
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
    @RequestMapping("/queryAppReportMetric")
    public RestResult queryAppReportMetric() {
        try {
            List<MetricResponse> list = new ArrayList<>();
            List<MetricEnum> metricEnumList = monitorService.getMetricEnumAll();
            if (!CollectionUtils.isEmpty(metricEnumList)) {
                metricEnumList.forEach(m -> list.add(getMetricResponse(m.getCode())));
            }
            list.addAll(getDiskHealth());
            return RestResult.restSuccess(monitorService.queryAppReportMetric(list));
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
        Health health = healthEndpoint.health();
        Map<String, Object> details = health.getDetails();
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

}