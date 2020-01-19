package org.dbsyncer.connector.redis;

import org.dbsyncer.connector.util.RedisUtil;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * 提供Redis集群API接口
 * @author AE86
 * @date 2018年5月28日 上午10:08:45
 * @version 1.0.0
 */
public class RedisTemplateCluster implements RedisTemplate {

    /**
     * 集群客户端
     */
    private JedisCluster cluster;

    public RedisTemplateCluster(JedisCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void set(byte[] key, byte[] val) {
        cluster.set(key, val);
    }

    @Override
    public byte[] get(byte[] key) {
        return cluster.get(key);
    }

    @Override
    public void del(byte[] key) {
        cluster.del(key);
    }

    @Override
    public void flushAll() {
        Map<String, JedisPool> nodes = cluster.getClusterNodes();
        for (Map.Entry<String, JedisPool> n : nodes.entrySet()) {
            JedisPool pool = n.getValue();
            pool.getResource().flushAll();
        }
    }

    @Override
    public void close() {
        // 关闭集群客户端
        RedisUtil.close(cluster);
    }
}
