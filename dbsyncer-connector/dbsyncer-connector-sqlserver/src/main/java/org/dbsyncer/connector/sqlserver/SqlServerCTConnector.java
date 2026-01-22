package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.sqlserver.ct.SqlServerCTListener;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL Server Change Tracking (CT) 连接器实现
 * 使用 Change Tracking 替代 CDC
 */
public class SqlServerCTConnector extends SqlServerConnector {

    @Override
    public String getConnectorType() {
        return "SqlServerCT";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            // 通过构造函数传入 SqlServerTemplate
            if (sqlTemplate instanceof SqlServerTemplate) {
                return new SqlServerCTListener((SqlServerTemplate) sqlTemplate);
            }
            throw new RuntimeException("SqlServerCTConnector 的 sqlTemplate 必须是 SqlServerTemplate 实例");
        }
        return null;
    }

    @Override
    public Map<String, String> getPosition(DatabaseConnectorInstance connectorInstance) throws Exception {
        // CT 模式使用版本号，而不是 LSN
        // 使用 SqlServerTemplate 的 buildGetCurrentVersionSql() 方法，避免硬编码 SQL
        Long currentVersion = connectorInstance.execute(databaseTemplate -> {
            assert sqlTemplate instanceof SqlServerTemplate;
            return databaseTemplate.queryForObject(((SqlServerTemplate) sqlTemplate).buildGetCurrentVersionSql(), Long.class);
        });

        if (currentVersion == null) {
            throw new RuntimeException("获取 SQL Server Change Tracking 当前版本号失败");
        }

        Map<String, String> position = new HashMap<>();
        position.put("version", String.valueOf(currentVersion));
        return position;
    }
}

