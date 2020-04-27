package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.monitor.Monitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/27 10:20
 */
@Service
public class MonitorServiceImpl implements MonitorService {

    @Autowired
    private Monitor monitor;

    @Override
    public Map getThreadInfo() {
        return monitor.getThreadInfo();
    }
}