package org.dbsyncer.monitor;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
public interface Monitor {

    boolean alive(String id);

    Map getThreadInfo();
}