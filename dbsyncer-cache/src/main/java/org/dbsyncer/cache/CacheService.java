package org.dbsyncer.cache;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 22:38
 */
public interface CacheService {

    /**
     * 存放K-V
     *
     * @param key
     * @param value
     * @return
     */
    Object put(String key, Object value);

    /**
     * 存放K-V，不存在k则写入
     *
     * @param key
     * @param value
     * @return
     */
    Object putIfAbsent(String key, Object value);

    /**
     * 根据Key删除
     *
     * @param key
     */
    void remove(String key);

    /**
     * 根据Key获取值
     *
     * @param key
     * @return
     */
    Object get(String key);

    /**
     * 获取所有值
     *
     * @return
     */
    Map<String, Object> getAll();

    /**
     * 根据Key获取值
     *
     * @param key
     * @param valueType
     * @param <T>
     * @return
     */
    <T> T get(String key, Class<T> valueType);

    /**
     * 清空缓存
     */
    void clear();

}
