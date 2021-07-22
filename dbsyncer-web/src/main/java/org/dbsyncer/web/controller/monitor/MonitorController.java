package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.monitor.model.Sample;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/monitor")
public class MonitorController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorService monitorService;

    @Autowired
    private MetricsEndpoint metricsEndpoint;

    @ResponseBody
    @RequestMapping("/get")
    public RestResult get() {
        try {
            List<MetricEnum> metricEnumList = monitorService.getMetricEnumAll();
            if (CollectionUtils.isEmpty(metricEnumList)) {
                return RestResult.restSuccess(Collections.EMPTY_LIST);
            }
            List<MetricResponse> list = new ArrayList<>();
            metricEnumList.forEach(m -> {
                MetricsEndpoint.MetricResponse metric = metricsEndpoint.metric(m.getCode(), null);
                MetricResponse metricResponse = new MetricResponse();
                metricResponse.setName(metric.getName());
                if (!CollectionUtils.isEmpty(metric.getMeasurements())) {
                    List<Sample> measurements = new ArrayList<>();
                    metric.getMeasurements().forEach(s -> measurements.add(new Sample(s.getStatistic().getTagValueRepresentation(), s.getValue())));
                    metricResponse.setMeasurements(measurements);
                }
                list.add(metricResponse);
            });
            return RestResult.restSuccess(monitorService.queryMetric(list));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        Map<String, String> params = getParams(request);
        model.put("threadInfo", monitorService.getThreadInfo());
        model.put("metaId", monitorService.getDefaultMetaId(params));
        model.put("meta", monitorService.getMetaAll());
        model.put("storageDataStatus", monitorService.getStorageDataStatusEnumAll());
        model.put("pagingData", monitorService.queryData(params));
        model.put("pagingLog", monitorService.queryLog(params));
        return "monitor/monitor.html";
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

}