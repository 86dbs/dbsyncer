package org.dbsyncer.biz;

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

}