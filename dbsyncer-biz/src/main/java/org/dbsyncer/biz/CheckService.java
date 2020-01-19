package org.dbsyncer.biz;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 22:58
 */
public interface CheckService {

    String checkConnector(Map<String, String> params);

    String checkMapping(Map<String, String> params);

}