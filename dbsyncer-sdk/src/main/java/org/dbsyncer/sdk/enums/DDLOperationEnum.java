package org.dbsyncer.sdk.enums;

/**
 * 支持同步的DDL命令
 *
 * @version 1.0.0
 * @Author life
 * @Date 2023-09-24 14:24
 */
public enum DDLOperationEnum {
    /**
     * 变更字段属性
     */
    ALTER_MODIFY,
    /**
     * 新增字段
     */
    ALTER_ADD,
    /**
     * 变更字段名称
     */
    ALTER_CHANGE,
    /**
     * 删除字段
     */
    ALTER_DROP;
}