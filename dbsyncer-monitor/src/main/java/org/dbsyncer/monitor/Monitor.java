package org.dbsyncer.monitor;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.monitor.enums.MetricEnum;
import org.dbsyncer.monitor.model.AppReportMetric;
import org.dbsyncer.monitor.model.MetricResponse;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
public interface Monitor {

    Mapping getMapping(String mappingId);

    List<Meta> getMetaAll();

    Paging queryData(String id, int pageNum, int pageSize, String error, String success);

    void clearData(String collectionId);

    Paging queryLog(int pageNum, int pageSize, String json);

    void clearLog();

    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();

    List<MetricEnum> getMetricEnumAll();

    List<MetricResponse> getThreadPoolInfo();

    AppReportMetric getAppReportMetric();

}