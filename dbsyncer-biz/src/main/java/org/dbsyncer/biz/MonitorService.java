package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.LogVo;
import org.dbsyncer.biz.vo.MetaVo;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/14 0:02
 */
public interface MonitorService {

    /**
     * 获取线程信息
     *
     * @return
     */
    Map getThreadInfo();

    /**
     * 获取驱动元信息列表
     *
     * @return
     */
    List<MetaVo> getMetaAll();

    /**
     * 获取驱动默认元信息id
     * @param params
     * @return
     */
    String getDefaultMetaId(Map<String, String> params);

    /**
     * 查询驱动同步数据
     *
     * @return
     * @param params
     */
    List<DataVo> queryData(Map<String, String> params);

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
    List<LogVo> queryLog(Map<String, String> params);

    /**
     * 清空操作日志
     *
     * @param id
     * @return
     */
    String clearLog(String type);

}