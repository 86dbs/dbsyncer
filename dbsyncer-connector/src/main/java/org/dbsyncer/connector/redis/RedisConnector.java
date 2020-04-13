package org.dbsyncer.connector.redis;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.template.CommandTemplate;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.RedisConfig;
import org.dbsyncer.connector.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RedisConnector implements Redis {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean isAlive(ConnectorConfig config) {
        RedisConfig cfg = (RedisConfig) config;
        RedisTemplate template = null;
        boolean r = false;
        try {
            // 打开连接
            template = this.getRedisTemplate(cfg, 1, 1, 1);
            r = true;
        } catch (Exception e) {
            logger.error("Failed to connect:{}", cfg.getUrl(), e.getLocalizedMessage());
        } finally {
            // 释放连接
            if (null != template) {
                template.close();
            }
        }
        return r;
    }

    @Override
    public List<String> getTable(ConnectorConfig config) {
        return null;
    }

    @Override
    public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
        return null;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandTemplate commandTemplate) {
        return null;
    }

    @Override
    public Map<String, String> getTargetCommand(CommandTemplate commandTemplate) {
        return null;
    }

    @Override
    public RedisTemplate getRedisTemplate(RedisConfig config) {
        return this.getRedisTemplate(config, null, null, null);
    }

    @Override
    public RedisTemplate getRedisTemplate(RedisConfig config, Integer maxTotal, Integer maxIdle, Integer minIdle) {
        // 获取节点地址
        List<String[]> servers = this.getServers(config.getUrl());
        if (null == servers || servers.isEmpty()) {
            logger.error("Url can not be null or invalid.");
            throw new IllegalArgumentException("Url can not be null or invalid.");
        }

        String password = config.getPassword();
        RedisTemplate redisTemplate = null;
        // 判断集群/单机
        int size = servers.size();
        if (1 < size) {
            JedisCluster cluster = RedisUtil.getJedisCluster(servers, password);
            redisTemplate = new RedisTemplateCluster(cluster);
        } else {
            String[] server = servers.get(0);
            String ip = server[0];
            int port = Integer.parseInt(server[1]);
            JedisPool pool = RedisUtil.getJedisPool(ip, port, password, maxTotal, maxIdle, minIdle);
            redisTemplate = new RedisTemplateSingle(pool);
        }
        return redisTemplate;
    }

    @Override
    public void close(RedisTemplate redisTemplate) {
        redisTemplate.close();
    }

    /**
     * 获取redis服务列表
     *
     * @param servers
     * @return List<String                                                                                                                                                                                                                                                               [                                                                                                                                                                                                                                                               ]> 服务列表
     */
    private List<String[]> getServers(String servers) {
        List<String[]> list = null;
        if (StringUtils.isNotEmpty(servers)) {
            list = new ArrayList<String[]>();
            // 服务列表以逗号分隔
            String[] splits = servers.split(",");
            for (String server : splits) {
                String[] node = server.split(":");
                if (node.length != 2) {
                    continue;
                }
                list.add(node);
            }
        }
        return list;
    }

}