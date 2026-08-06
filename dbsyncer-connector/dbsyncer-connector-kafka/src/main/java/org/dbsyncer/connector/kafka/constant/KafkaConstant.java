/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.constant;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 22:30
 */
public final class KafkaConstant {

    public static final String TOPIC = "topic";

    /**
     * 消息体：源表名
     */
    public static final String FIELD_TABLE = "table";

    /**
     * 消息体：操作类型（INSERT/UPDATE/DELETE）
     */
    public static final String FIELD_EVENT = "event";

    /**
     * 消息体：行数据
     */
    public static final String FIELD_DATA = "data";

    private KafkaConstant() {
    }
}
