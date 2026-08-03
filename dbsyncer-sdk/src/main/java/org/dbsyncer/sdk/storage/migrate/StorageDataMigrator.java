/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.migrate;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储结构兼容升级（临时）：混存 config 拆表、STATUS→meta、嵌套表映射物化、预建表级 Meta / task_detail。
 * <p><b>兼容若干版本后可整包删除</b>（含 H2/MySQL 子类与 initTable 调用）。不迁旧明细数据、不 DROP 旧表。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-03 10:45
 */
public abstract class StorageDataMigrator {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String T_CONFIG = "dbsyncer_config";
    private static final String T_USER = "dbsyncer_user";
    private static final String T_CONNECTOR = "dbsyncer_connector";
    private static final String T_TASK = "dbsyncer_task";
    private static final String T_MAPPING = "dbsyncer_mapping";
    private static final String T_TABLE_GROUP = "dbsyncer_table_group";
    private static final String T_META = "dbsyncer_meta";

    private final StorageService storage;

    protected StorageDataMigrator(StorageService storage) {
        this.storage = storage;
    }

    /**
     * 启动入口：有旧数据则拆分；随后幂等物化嵌套表映射并预建 detail 分表。
     */
    public final void run() {
        try {
            if (needSplit()) {
                logger.warn("执行存储兼容升级（临时脚本），请确认已备份；旧表不删除、旧明细不迁移");
                splitConfig();
                migrateStandaloneMapping();
                migrateTaskStatusToMeta();
                logger.info("存储兼容升级：config 拆分完成");
            }
            materializeNestedTableMappings();
            // 每个 table_group 需有表级 Meta（TASK_ID=table_group.id, IS_TASK_DETAIL=1），校验/整库启动靠它筛选未完成表
            ensureTableGroupDetailMetas();
            ensureTaskDetailTables();
        } catch (Exception e) {
            logger.error("存储兼容升级失败: {}", e.getMessage(), e);
        }
    }

    private boolean needSplit() {
        if (tableExists(T_CONFIG) && countConfigTypes(ConfigConstant.USER, ConfigConstant.CONNECTOR,
                ConfigConstant.MAPPING, ConfigConstant.TABLE_GROUP, ConfigConstant.META) > 0) {
            return true;
        }
        if (tableExists(T_MAPPING) && count("SELECT COUNT(1) FROM " + q(T_MAPPING)) > 0) {
            return true;
        }
        return tableExists(T_TASK) && columnExists(T_TASK, "STATUS");
    }

    private void splitConfig() {
        if (!tableExists(T_CONFIG)) {
            return;
        }
        for (Map<String, Object> row : query("SELECT * FROM " + q(T_CONFIG))) {
            switch (val(row, "type", "TYPE")) {
                case ConfigConstant.USER:
                    migrateUser(row);
                    break;
                case ConfigConstant.CONNECTOR:
                    migrateConnector(row);
                    break;
                case ConfigConstant.MAPPING:
                    migrateTask(row, ConfigConstant.MAPPING);
                    break;
                case ConfigConstant.TABLE_GROUP:
                    migrateTableGroup(row);
                    break;
                case ConfigConstant.META:
                    migrateMeta(row);
                    break;
                default:
                    break;
            }
        }
    }

    private void migrateStandaloneMapping() {
        if (!tableExists(T_MAPPING)) {
            return;
        }
        for (Map<String, Object> row : query("SELECT * FROM " + q(T_MAPPING))) {
            migrateTask(row, ConfigConstant.MAPPING);
        }
    }

    private void migrateUser(Map<String, Object> row) {
        String json = val(row, "json", "JSON");
        if (StringUtil.isBlank(json)) {
            return;
        }
        Map root = JsonUtil.parseMap(json);
        if (root == null) {
            return;
        }
        Object listObj = root.get("userInfoList");
        if (listObj == null) {
            listObj = root.get("userInfo");
        }
        List<Map> users;
        if (listObj instanceof List) {
            users = (List<Map>) listObj;
        } else if (root.containsKey("username")) {
            users = Collections.singletonList(root);
        } else {
            return;
        }
        long now = System.currentTimeMillis();
        long createTime = num(row, "createTime", "CREATE_TIME", now);
        long updateTime = num(row, "updateTime", "UPDATE_TIME", now);
        for (Map user : users) {
            if (user == null) {
                continue;
            }
            String username = val(user, "username");
            if (StringUtil.isBlank(username)) {
                continue;
            }
            String id = first(val(user, "id"), UUIDUtil.getUUID());
            if (existsId(T_USER, id) || existsUser(username)) {
                continue;
            }
            Map<String, Object> p = new HashMap<>();
            p.put(ConfigConstant.CONFIG_MODEL_ID, id);
            p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(user, "createTime", createTime));
            p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, num(user, "updateTime", updateTime));
            p.put(ConfigConstant.USER_USERNAME, username);
            p.put(ConfigConstant.USER_PASSWORD, val(user, "password"));
            p.put(ConfigConstant.USER_NICKNAME, first(val(user, "nickname"), username));
            p.put(ConfigConstant.USER_ROLE, first(val(user, "roleCode"), val(user, "role"), "admin"));
            p.put(ConfigConstant.USER_EMAIL, StringUtil.getIfBlank(val(user, "email"), StringUtil.EMPTY));
            p.put(ConfigConstant.USER_PHONE, StringUtil.getIfBlank(val(user, "phone"), StringUtil.EMPTY));
            storage.add(StorageEnum.USER, p);
        }
    }

    private void migrateConnector(Map<String, Object> row) {
        String id = val(row, "id", "ID");
        if (StringUtil.isBlank(id) || existsId(T_CONNECTOR, id)) {
            return;
        }
        String json = val(row, "json", "JSON");
        Map root = StringUtil.isBlank(json) ? new HashMap() : JsonUtil.parseMap(json);
        if (root == null) {
            root = new HashMap();
        }
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, id);
        p.put(ConfigConstant.CONFIG_MODEL_NAME, first(val(row, "name", "NAME"), val(root, "name"), id));
        p.put(ConfigConstant.CONFIG_MODEL_TYPE, resolveConnectorType(root, val(row, "type", "TYPE")));
        p.put(ConfigConstant.CONFIG_MODEL_JSON, StringUtil.isBlank(json) ? "{}" : json);
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(row, "createTime", "CREATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, num(row, "updateTime", "UPDATE_TIME", System.currentTimeMillis()));
        storage.add(StorageEnum.CONNECTOR, p);
    }

    private String resolveConnectorType(Map root, String fallback) {
        String type = val(root, "connectorType");
        if (StringUtil.isNotBlank(type) && !ConfigConstant.CONNECTOR.equals(type)) {
            return type;
        }
        Object config = root.get("config");
        if (config instanceof Map) {
            type = val((Map) config, "connectorType");
            if (StringUtil.isNotBlank(type)) {
                return type;
            }
        }
        type = val(root, "type");
        if (StringUtil.isNotBlank(type) && !ConfigConstant.CONNECTOR.equals(type)) {
            return type;
        }
        return StringUtil.isBlank(fallback) || ConfigConstant.CONNECTOR.equals(fallback) ? "mysql" : fallback;
    }

    private void migrateTask(Map<String, Object> row, String defaultType) {
        String id = val(row, "id", "ID");
        if (StringUtil.isBlank(id) || existsId(T_TASK, id)) {
            return;
        }
        String json = rewriteMappingId(val(row, "json", "JSON"));
        String type = first(val(row, "type", "TYPE"), defaultType);
        if (StringUtil.isBlank(type) || ConfigConstant.MAPPING.equals(type)) {
            type = ConfigConstant.MAPPING;
        }
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, id);
        p.put(ConfigConstant.CONFIG_MODEL_NAME, first(val(row, "name", "NAME"), id));
        p.put(ConfigConstant.CONFIG_MODEL_TYPE, type);
        p.put(ConfigConstant.CONFIG_MODEL_JSON, StringUtil.isBlank(json) ? "{}" : json);
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(row, "createTime", "CREATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, num(row, "updateTime", "UPDATE_TIME", System.currentTimeMillis()));
        storage.add(StorageEnum.TASK, p);
    }

    private void migrateTableGroup(Map<String, Object> row) {
        String id = val(row, "id", "ID");
        if (StringUtil.isBlank(id) || existsId(T_TABLE_GROUP, id)) {
            return;
        }
        String json = val(row, "json", "JSON");
        Map root = StringUtil.isBlank(json) ? new HashMap() : JsonUtil.parseMap(json);
        if (root == null) {
            root = new HashMap();
        }
        rewriteMappingId(root);
        String taskId = first(val(root, "taskId"), val(root, "mappingId"), id);
        String sourceTable = tableName(root.get("sourceTable"));
        String targetTable = tableName(root.get("targetTable"));
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, id);
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(row, "createTime", "CREATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, num(row, "updateTime", "UPDATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        p.put(ConfigConstant.TABLE_GROUP_SORT_INDEX, (int) num(root, "index", 0));
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID, blank(val(root, "sourceConnectorId")));
        p.put(ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID, blank(val(root, "targetConnectorId")));
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, blank(val(root, "sourceDatabase")));
        p.put(ConfigConstant.TABLE_GROUP_TARGET_DATABASE, blank(val(root, "targetDatabase")));
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, blank(val(root, "sourceSchema")));
        p.put(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA, blank(val(root, "targetSchema")));
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, blank(sourceTable));
        p.put(ConfigConstant.TABLE_GROUP_TARGET_TABLE, blank(targetTable));
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, num(root, "sourceTotal", 0));
        p.put(ConfigConstant.TABLE_GROUP_TARGET_TOTAL, num(root, "targetTotal", 0));
        p.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJsonSafe(root));
        storage.add(StorageEnum.TABLE_GROUP, p);
        ensureDetailMeta(id);
    }

    private void migrateMeta(Map<String, Object> row) {
        String id = val(row, "id", "ID");
        if (StringUtil.isBlank(id) || existsId(T_META, id)) {
            return;
        }
        String json = val(row, "json", "JSON");
        Map root = StringUtil.isBlank(json) ? new HashMap(row) : JsonUtil.parseMap(json);
        if (root == null) {
            root = new HashMap(row);
        }
        rewriteMappingId(root);
        Object snapshot = root.get("snapshot");
        String snapshotJson = snapshot instanceof String ? (String) snapshot
                : snapshot != null ? JsonUtil.objToJsonSafe(snapshot) : "{}";
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, id);
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(row, "createTime", "CREATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, num(row, "updateTime", "UPDATE_TIME", System.currentTimeMillis()));
        p.put(ConfigConstant.META_TASK_ID, first(val(root, "taskId"), val(root, "mappingId"), id));
        p.put(ConfigConstant.META_STATE, (int) num(root, "state", 0));
        p.put(ConfigConstant.META_IS_TASK_DETAIL, (int) num(root, "isTaskDetail", 0));
        p.put(ConfigConstant.META_TOTAL, atomic(root, "total"));
        p.put(ConfigConstant.META_SUCCESS, atomic(root, "success"));
        p.put(ConfigConstant.META_FAIL, atomic(root, "fail"));
        p.put(ConfigConstant.META_DIFF, atomic(root, "diff"));
        p.put(ConfigConstant.META_FIXED, atomic(root, "fixed"));
        p.put(ConfigConstant.META_SNAPSHOT, snapshotJson);
        storage.add(StorageEnum.META, p);
    }

    /** 仅补缺：已有任务级 meta 不覆盖（config.meta.state 优先）。 */
    private void migrateTaskStatusToMeta() {
        if (!tableExists(T_TASK) || !columnExists(T_TASK, "STATUS") || !tableExists(T_META)) {
            return;
        }
        for (Map<String, Object> row : query("SELECT " + q("ID") + "," + q("STATUS") + " FROM " + q(T_TASK))) {
            String taskId = val(row, "id", "ID");
            if (StringUtil.isBlank(taskId) || hasTaskMeta(taskId)) {
                continue;
            }
            int old = (int) num(row, "status", "STATUS", 0);
            int state = (old == 2 || old == 3) ? 3 : 0;
            long now = System.currentTimeMillis();
            Map<String, Object> p = new HashMap<>();
            p.put(ConfigConstant.CONFIG_MODEL_ID, UUIDUtil.getUUID());
            p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, now);
            p.put(ConfigConstant.META_TASK_ID, taskId);
            p.put(ConfigConstant.META_STATE, state);
            p.put(ConfigConstant.META_IS_TASK_DETAIL, 0);
            p.put(ConfigConstant.META_TOTAL, 0L);
            p.put(ConfigConstant.META_SUCCESS, 0L);
            p.put(ConfigConstant.META_FAIL, 0L);
            p.put(ConfigConstant.META_DIFF, 0L);
            p.put(ConfigConstant.META_FIXED, 0L);
            p.put(ConfigConstant.META_SNAPSHOT, "{}");
            storage.add(StorageEnum.META, p);
        }
    }

    private void materializeNestedTableMappings() {
        if (!tableExists(T_TASK) || !tableExists(T_TABLE_GROUP)) {
            return;
        }
        for (Map<String, Object> task : query("SELECT * FROM " + q(T_TASK))) {
            String taskId = val(task, "id", "ID");
            String json = val(task, "json", "JSON");
            if (StringUtil.isBlank(taskId) || StringUtil.isBlank(json)) {
                continue;
            }
            Map root = JsonUtil.parseMap(json);
            if (root == null) {
                continue;
            }
            rewriteMappingId(root);
            Object mappingsObj = root.get("databaseMappings");
            if (!(mappingsObj instanceof List) || CollectionUtils.isEmpty((List) mappingsObj)) {
                continue;
            }
            boolean changed = false;
            int sort = (int) count("SELECT COALESCE(MAX(" + q("SORT_INDEX") + "),0) FROM " + q(T_TABLE_GROUP)
                    + " WHERE " + q("TASK_ID") + "=?", taskId);
            for (Object mappingObj : (List) mappingsObj) {
                if (!(mappingObj instanceof Map)) {
                    continue;
                }
                Map mapping = (Map) mappingObj;
                Object tablesObj = mapping.get("tableMappings");
                if (!(tablesObj instanceof List) || CollectionUtils.isEmpty((List) tablesObj)) {
                    continue;
                }
                String sc = blank(val(mapping, "sourceConnectorId"));
                String tc = blank(val(mapping, "targetConnectorId"));
                String sd = blank(val(mapping, "sourceDatabase"));
                String td = blank(val(mapping, "targetDatabase"));
                String ss = blank(val(mapping, "sourceSchema"));
                String ts = blank(val(mapping, "targetSchema"));
                for (Object tableObj : (List) tablesObj) {
                    if (!(tableObj instanceof Map)) {
                        continue;
                    }
                    Map tm = (Map) tableObj;
                    String st = tableName(tm.get("sourceTable"));
                    String tt = tableName(tm.get("targetTable"));
                    if (StringUtil.isBlank(st) || StringUtil.isBlank(tt) || existsTableGroup(taskId, sc, tc, sd, td, ss, ts, st, tt)) {
                        continue;
                    }
                    sort++;
                    insertTableGroup(taskId, sort, sc, tc, sd, td, ss, ts, st, tt, (int) num(tm, "index", sort));
                    changed = true;
                }
                mapping.remove("tableMappings");
                changed = true;
            }
            if (changed) {
                Map<String, Object> p = new HashMap<>();
                p.put(ConfigConstant.CONFIG_MODEL_ID, taskId);
                p.put(ConfigConstant.CONFIG_MODEL_NAME, first(val(task, "name", "NAME"), taskId));
                p.put(ConfigConstant.CONFIG_MODEL_TYPE, first(val(task, "type", "TYPE"), ConfigConstant.MAPPING));
                p.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJsonSafe(root));
                p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, num(task, "createTime", "CREATE_TIME", System.currentTimeMillis()));
                p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, System.currentTimeMillis());
                storage.edit(StorageEnum.TASK, p);
            }
        }
    }

    private void insertTableGroup(String taskId, int sort, String sc, String tc, String sd, String td,
                                  String ss, String ts, String st, String tt, int index) {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("name", st);
        src.put("type", TableTypeEnum.TABLE.getCode());
        Map<String, Object> tgt = new LinkedHashMap<>();
        tgt.put("name", tt);
        tgt.put("type", TableTypeEnum.TABLE.getCode());
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("taskId", taskId);
        root.put("index", index > 0 ? index : sort);
        root.put("sourceConnectorId", sc);
        root.put("targetConnectorId", tc);
        root.put("sourceDatabase", sd);
        root.put("targetDatabase", td);
        root.put("sourceSchema", ss);
        root.put("targetSchema", ts);
        root.put("sourceTable", src);
        root.put("targetTable", tgt);
        long now = System.currentTimeMillis();
        String id = UUIDUtil.getUUID();
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, id);
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, now);
        p.put(ConfigConstant.TABLE_GROUP_TASK_ID, taskId);
        p.put(ConfigConstant.TABLE_GROUP_SORT_INDEX, sort);
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID, sc);
        p.put(ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID, tc);
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, sd);
        p.put(ConfigConstant.TABLE_GROUP_TARGET_DATABASE, td);
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, ss);
        p.put(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA, ts);
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, st);
        p.put(ConfigConstant.TABLE_GROUP_TARGET_TABLE, tt);
        p.put(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, 0L);
        p.put(ConfigConstant.TABLE_GROUP_TARGET_TOTAL, 0L);
        p.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJsonSafe(root));
        storage.add(StorageEnum.TABLE_GROUP, p);
        ensureDetailMeta(id);
    }

    /**
     * 为每个 table_group 补表级 Meta：TASK_ID=table_group.id，IS_TASK_DETAIL=1，STATE=0。
     * <p>ValidateSync / DatabaseSync 启动靠 dm JOIN dtg ON dtg.ID = dm.TASK_ID 拉取未完成表。
     */
    private void ensureTableGroupDetailMetas() {
        if (!tableExists(T_TABLE_GROUP) || !tableExists(T_META)) {
            return;
        }
        for (Map<String, Object> row : query("SELECT " + q("ID") + " FROM " + q(T_TABLE_GROUP))) {
            String tableGroupId = val(row, "id", "ID");
            if (StringUtil.isNotBlank(tableGroupId)) {
                ensureDetailMeta(tableGroupId);
            }
        }
    }

    private void ensureDetailMeta(String tableGroupId) {
        if (StringUtil.isBlank(tableGroupId) || !tableExists(T_META) || hasDetailMeta(tableGroupId)) {
            return;
        }
        long now = System.currentTimeMillis();
        Map<String, Object> p = new HashMap<>();
        p.put(ConfigConstant.CONFIG_MODEL_ID, UUIDUtil.getUUID());
        p.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
        p.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, now);
        p.put(ConfigConstant.META_TASK_ID, tableGroupId);
        p.put(ConfigConstant.META_STATE, 0);
        p.put(ConfigConstant.META_IS_TASK_DETAIL, 1);
        p.put(ConfigConstant.META_TOTAL, 0L);
        p.put(ConfigConstant.META_SUCCESS, 0L);
        p.put(ConfigConstant.META_FAIL, 0L);
        p.put(ConfigConstant.META_DIFF, 0L);
        p.put(ConfigConstant.META_FIXED, 0L);
        p.put(ConfigConstant.META_SNAPSHOT, "{}");
        storage.add(StorageEnum.META, p);
    }

    private void ensureTaskDetailTables() {
        if (!tableExists(T_TASK)) {
            return;
        }
        for (Map<String, Object> task : query("SELECT " + q("ID") + " FROM " + q(T_TASK))) {
            String taskId = val(task, "id", "ID");
            if (StringUtil.isBlank(taskId)) {
                continue;
            }
            try {
                storage.ensure(StorageEnum.TASK_DETAIL, taskId);
            } catch (Exception e) {
                logger.warn("预建 task_detail 失败 {}: {}", taskId, e.getMessage());
            }
        }
    }

    private boolean existsTableGroup(String taskId, String sc, String tc, String sd, String td,
                                     String ss, String ts, String st, String tt) {
        return count("SELECT COUNT(1) FROM " + q(T_TABLE_GROUP)
                        + " WHERE " + q("TASK_ID") + "=? AND " + q("SOURCE_CONNECTOR_ID") + "=? AND "
                        + q("TARGET_CONNECTOR_ID") + "=? AND " + q("SOURCE_DATABASE") + "=? AND "
                        + q("TARGET_DATABASE") + "=? AND " + q("SOURCE_SCHEMA") + "=? AND "
                        + q("TARGET_SCHEMA") + "=? AND " + q("SOURCE_TABLE") + "=? AND " + q("TARGET_TABLE") + "=?",
                taskId, sc, tc, sd, td, ss, ts, st, tt) > 0;
    }

    private boolean hasTaskMeta(String taskId) {
        return count("SELECT COUNT(1) FROM " + q(T_META) + " WHERE " + q("TASK_ID") + "=? AND "
                + q("IS_TASK_DETAIL") + "=0", taskId) > 0;
    }

    private boolean hasDetailMeta(String tableGroupId) {
        return count("SELECT COUNT(1) FROM " + q(T_META) + " WHERE " + q("TASK_ID") + "=? AND "
                + q("IS_TASK_DETAIL") + "=1", tableGroupId) > 0;
    }

    private long countConfigTypes(String... types) {
        if (types == null || types.length == 0 || !tableExists(T_CONFIG)) {
            return 0L;
        }
        StringBuilder in = new StringBuilder();
        List<Object> args = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            if (i > 0) {
                in.append(',');
            }
            in.append('?');
            args.add(types[i]);
        }
        return count("SELECT COUNT(1) FROM " + q(T_CONFIG) + " WHERE " + q("TYPE") + " IN (" + in + ")", args.toArray());
    }

    private boolean existsId(String table, String id) {
        return StringUtil.isNotBlank(id) && tableExists(table)
                && count("SELECT COUNT(1) FROM " + q(table) + " WHERE " + q("ID") + "=?", id) > 0;
    }

    private boolean existsUser(String username) {
        return StringUtil.isNotBlank(username) && tableExists(T_USER)
                && count("SELECT COUNT(1) FROM " + q(T_USER) + " WHERE " + q("USERNAME") + "=?", username) > 0;
    }

    private String rewriteMappingId(String json) {
        if (StringUtil.isBlank(json)) {
            return json;
        }
        Map map = JsonUtil.parseMap(json);
        if (map == null) {
            return json.replace("\"mappingId\"", "\"taskId\"");
        }
        rewriteMappingId(map);
        return JsonUtil.objToJsonSafe(map);
    }

    private void rewriteMappingId(Map map) {
        if (map == null) {
            return;
        }
        if (map.containsKey("mappingId") && !map.containsKey("taskId")) {
            map.put("taskId", map.remove("mappingId"));
        }
        for (Object v : new ArrayList<>(map.values())) {
            if (v instanceof Map) {
                rewriteMappingId((Map) v);
            } else if (v instanceof List) {
                for (Object item : (List) v) {
                    if (item instanceof Map) {
                        rewriteMappingId((Map) item);
                    }
                }
            }
        }
    }

    private String tableName(Object obj) {
        if (obj == null) {
            return StringUtil.EMPTY;
        }
        if (obj instanceof String) {
            return StringUtil.trim((String) obj);
        }
        if (obj instanceof Map) {
            return first(val((Map) obj, "name"), val((Map) obj, "tableName"));
        }
        return StringUtil.EMPTY;
    }

    private List<Map<String, Object>> query(String sql, Object... args) {
        List<Map<String, Object>> rows = rawQuery(sql, args);
        return rows == null ? Collections.emptyList() : rows;
    }

    private long count(String sql, Object... args) {
        Long cnt = rawCount(sql, args);
        return cnt == null ? 0L : cnt;
    }

    private String q(String name) {
        return quote(name);
    }

    private static String blank(String v) {
        return StringUtil.getIfBlank(v, StringUtil.EMPTY);
    }

    private static String val(Map<?, ?> map, String... keys) {
        if (map == null || keys == null) {
            return StringUtil.EMPTY;
        }
        for (String key : keys) {
            Object v = map.get(key);
            if (v != null && StringUtil.isNotBlank(String.valueOf(v))) {
                return String.valueOf(v).trim();
            }
        }
        return StringUtil.EMPTY;
    }

    private static String first(String... values) {
        if (values == null) {
            return StringUtil.EMPTY;
        }
        for (String v : values) {
            if (StringUtil.isNotBlank(v)) {
                return v;
            }
        }
        return StringUtil.EMPTY;
    }

    private static long num(Map<?, ?> map, String key, long def) {
        return num(map, key, null, def);
    }

    private static long num(Map<?, ?> map, String k1, String k2, long def) {
        Object v = map == null ? null : map.get(k1);
        if (v == null && k2 != null && map != null) {
            v = map.get(k2);
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return v == null ? def : NumberUtil.toLong(String.valueOf(v), def);
    }

    private static long atomic(Map<?, ?> map, String key) {
        Object v = map == null ? null : map.get(key);
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        if (v instanceof Map) {
            Object nested = ((Map<?, ?>) v).get("value");
            if (nested instanceof Number) {
                return ((Number) nested).longValue();
            }
            return nested == null ? 0L : NumberUtil.toLong(String.valueOf(nested), 0L);
        }
        return v == null ? 0L : NumberUtil.toLong(String.valueOf(v), 0L);
    }

    protected abstract boolean tableExists(String tableName);

    protected abstract boolean columnExists(String tableName, String columnName);

    protected abstract List<Map<String, Object>> rawQuery(String sql, Object... args);

    protected abstract Long rawCount(String sql, Object... args);

    protected abstract String quote(String name);
}
