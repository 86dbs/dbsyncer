package org.dbsyncer.connector.redis;

/**
 * 提供Redis单机/集群统一API接口
 * @author AE86
 * @date 2018年5月28日 上午9:48:17
 * @version 1.0.0
 */
public interface RedisTemplate {

    /**
     * 插入key值
     * @param key
     * @param val
     */
    void set(byte[] key, byte[] val);

    /**
     * 获取key值
     * @param key
     * @return
     */
    byte[] get(byte[] key);

    /**
     * 删除指定key
     * @param key
     */
    void del(byte[] key);

    /**
     * 清空所有数据
     */
    void flushAll();
    
    /**
     * 释放连接
     */
    void close();
}
