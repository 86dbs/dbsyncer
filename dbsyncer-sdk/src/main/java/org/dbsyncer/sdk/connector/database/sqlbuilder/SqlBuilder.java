/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sqlbuilder;

import org.dbsyncer.sdk.config.SqlBuilderConfig;

/**
 * SQL生成器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/26 23:49
 */
public interface SqlBuilder {

    /**
     * 生成SQL
     * @param config
     * @return
     */
    String buildSql(SqlBuilderConfig config);

    /**
     * 生成查询SQL
     * @param config
     * @return
     */
    String buildQuerySql(SqlBuilderConfig config);
}