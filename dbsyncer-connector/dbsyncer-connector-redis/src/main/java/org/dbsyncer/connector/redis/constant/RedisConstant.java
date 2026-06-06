/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.constant;

/**
 * Redis 连接器常量
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06
 */
public final class RedisConstant {

    private RedisConstant() {
    }

    /** 缓存 key 前缀 */
    public static final String KEY_PREFIX = "keyPrefix";

    /** 复合主键连接符 */
    public static final String KEY_JOINER = "keyJoiner";

    /** 数据结构 */
    public static final String DATA_STRUCTURE = "dataStructure";

    /** Stream 消费组 */
    public static final String GROUP_ID = "groupId";

    /** Stream 消费者名称 */
    public static final String CONSUMER_NAME = "consumerName";

    /** 过期策略 */
    public static final String EXPIRE_TYPE = "expireType";

    /** 过期时间（秒） */
    public static final String EXPIRE_SECONDS = "expireSeconds";

    /** 永不过期 */
    public static final String EXPIRE_NEVER = "never";

    /** 到期过期 */
    public static final String EXPIRE = "expire";

    /** String 数据结构 */
    public static final String DATA_TYPE_STRING = "String";
}
