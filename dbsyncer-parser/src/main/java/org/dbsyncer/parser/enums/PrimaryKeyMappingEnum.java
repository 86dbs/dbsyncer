/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.parser.strategy.PrimaryKeyMappingStrategy;
import org.dbsyncer.parser.strategy.impl.OraclePrimaryKeyMappingStrategy;

/**
 * 主键映射策略枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/10/19 14:19
 */
public enum PrimaryKeyMappingEnum {

    /**
     * Oracle
     */
    ORACLE(ConnectorEnum.ORACLE.getType(), new OraclePrimaryKeyMappingStrategy());

    private String type;

    private PrimaryKeyMappingStrategy primaryKeyMappingStrategy;

    PrimaryKeyMappingEnum(String type, PrimaryKeyMappingStrategy primaryKeyMappingStrategy) {
        this.type = type;
        this.primaryKeyMappingStrategy = primaryKeyMappingStrategy;
    }

    /**
     * 主键映射策略
     *
     * @param type
     * @return
     */
    public static PrimaryKeyMappingStrategy getPrimaryKeyMappingStrategy(String type) {
        for (PrimaryKeyMappingEnum e : PrimaryKeyMappingEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getPrimaryKeyMappingStrategy();
            }
        }
        return new PrimaryKeyMappingStrategy() {};
    }

    public String getType() {
        return type;
    }

    public PrimaryKeyMappingStrategy getPrimaryKeyMappingStrategy() {
        return primaryKeyMappingStrategy;
    }
}