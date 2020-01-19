package org.dbsyncer.connector.redis;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.RedisConfig;

public interface Redis extends Connector {

    /**
     * 获取redis连接实例
     *
     * @param config
     * @return
     */
    RedisTemplate getRedisTemplate(RedisConfig config);

    /**
     * 获取redis连接实例
     *
     * @param config
     * @param maxTotal 最大连接数
     * @param maxIdle  在jedispool中最大的idle状态(空闲的)的jedis实例的个数
     * @param minIdle  在jedispool中最小的idle状态(空闲的)的jedis实例的个数
     * @return
     */
    RedisTemplate getRedisTemplate(RedisConfig config, Integer maxTotal, Integer maxIdle, Integer minIdle);

    /**
     * 关闭连接
     *
     * @param redisTemplate
     */
    void close(RedisTemplate redisTemplate);

}
