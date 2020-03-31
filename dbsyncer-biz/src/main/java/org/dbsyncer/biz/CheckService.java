package org.dbsyncer.biz;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 22:58
 */
public interface CheckService {

    /**
     * 检查配置
     *
     * @param params 参数
     * @param type 返回类型
     * @param <T>
     * @return
     */
    <T> T check(Map<String, String> params, Class<T> type);

}