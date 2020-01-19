package org.dbsyncer.connector.redis;

import org.dbsyncer.connector.util.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 提供Redis单机API接口
 * @author AE86
 * @date 2018年5月28日 上午10:08:26
 * @version 1.0.0
 */
public class RedisTemplateSingle implements RedisTemplate {

    /**
     * 连接池
     */
    private JedisPool pool;

    public RedisTemplateSingle(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public void set(byte[] key, byte[] val) {
        // 获取一个连接管道
        Jedis jedis = pool.getResource();
        jedis.set(key, val);
        // 关闭连接管道
        RedisUtil.close(jedis);
    }

    @Override
    public byte[] get(byte[] key) {
        Jedis jedis = pool.getResource();
        byte[] bs = jedis.get(key);
        RedisUtil.close(jedis);
        return bs;
    }

    @Override
    public void del(byte[] key) {
        Jedis jedis = pool.getResource();
        jedis.del(key);
        RedisUtil.close(jedis);
    }

    @Override
    public void flushAll() {
        Jedis jedis = pool.getResource();
        jedis.flushAll();
        RedisUtil.close(jedis);
    }

    @Override
    public void close() {
        // 释放连接池
        RedisUtil.close(pool);
    }

}
