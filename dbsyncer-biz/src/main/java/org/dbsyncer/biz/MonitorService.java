/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.enums.MetricEnum;
import org.dbsyncer.biz.model.AppReportMetric;
import org.dbsyncer.biz.model.MetricResponse;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/14 0:02
 */
public interface MonitorService {

    /**
     * 获取驱动元信息列表
     *
     * @return
     */
    List<MetaVo> getMetaAll();

    /**
     * 获取驱动元信息
     *
     * @param metaId
     * @return
     */
    MetaVo getMetaVo(String metaId);

    /**
     * 获取驱动默认元信息id
     *
     * @param params
     * @return
     */
    String getDefaultMetaId(Map<String, String> params);

    /**
     * 查询驱动同步数据
     *
     * @param params
     * @return
     */
    Paging queryData(Map<String, String> params);

    /**
     * 清空驱动同步数据
     *
     * @param id
     * @return
     */
    String clearData(String id);

    /**
     * 查询操作日志
     *
     * @param params
     * @return
     */
    Paging queryLog(Map<String, String> params);

    /**
     * 清空操作日志
     *
     * @return
     */
    String clearLog();

    /**
     * 删除过期的数据和日志
     */
    void deleteExpiredDataAndLog();

    /**
     * 获取所有同步数据状态类型
     *
     * @return
     */
    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();

    /**
     * 获取监控系统指标
     *
     * @return
     */
    List<MetricEnum> getMetricEnumAll();

    /**
     * 获取应用报告
     *
     * @return
     */
    AppReportMetric queryAppReportMetric(List<MetricResponse> metrics);

}