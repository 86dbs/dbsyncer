/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.storage;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.sdk.storage.migrate.StorageDataMigrator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MySQL 存储兼容升级（临时，随 StorageDataMigrator 一并可删）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-03 10:45
 */
public class MySQLStorageDataMigrator extends StorageDataMigrator {

    private final DatabaseConnectorInstance connectorInstance;
    private final String database;

    public MySQLStorageDataMigrator(StorageService storageService, DatabaseConnectorInstance connectorInstance, String database) {
        super(storageService);
        this.connectorInstance = connectorInstance;
        this.database = database;
    }

    @Override
    protected boolean tableExists(String tableName) {
        if (StringUtil.isBlank(tableName) || StringUtil.isBlank(database)) {
            return false;
        }
        try {
            String sql = "SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
            Long cnt = connectorInstance.execute(tpl -> tpl.queryForObject(sql, new Object[]{database, tableName}, Long.class));
            return cnt != null && cnt > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected boolean columnExists(String tableName, String columnName) {
        if (StringUtil.isBlank(tableName) || StringUtil.isBlank(columnName) || StringUtil.isBlank(database)) {
            return false;
        }
        try {
            String sql = "SELECT COUNT(1) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?";
            Long cnt = connectorInstance.execute(tpl -> tpl.queryForObject(sql, new Object[]{database, tableName, columnName}, Long.class));
            return cnt != null && cnt > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected List<Map<String, Object>> rawQuery(String sql, Object... args) {
        try {
            List<Map<String, Object>> rows = connectorInstance.execute(tpl -> tpl.queryForList(sql, args));
            return CollectionUtils.isEmpty(rows) ? new ArrayList<>() : rows;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    @Override
    protected Long rawCount(String sql, Object... args) {
        try {
            return connectorInstance.execute(tpl -> tpl.queryForObject(sql, args, Long.class));
        } catch (Exception e) {
            return 0L;
        }
    }

    @Override
    protected String quote(String name) {
        return StringUtil.BACK_QUOTE + name + StringUtil.BACK_QUOTE;
    }
}
