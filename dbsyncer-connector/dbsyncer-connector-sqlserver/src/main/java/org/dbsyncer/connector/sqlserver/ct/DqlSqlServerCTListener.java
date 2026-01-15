package org.dbsyncer.connector.sqlserver.ct;

import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;

/**
 * DQL SQL Server Change Tracking (CT) 监听器实现
 */
public class DqlSqlServerCTListener extends SqlServerCTListener {

    /**
     * 构造函数
     *
     * @param sqlTemplate SQL Server 模板实例，用于构建 SQL 语句
     */
    public DqlSqlServerCTListener(SqlServerTemplate sqlTemplate) {
        super(sqlTemplate);
    }

    // DQL 模式下的 CT 监听器，继承自 SqlServerCTListener
    // 如果需要特殊处理，可以在这里重写方法
}

