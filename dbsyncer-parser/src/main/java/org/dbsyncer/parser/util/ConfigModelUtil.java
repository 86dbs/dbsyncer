package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.model.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/19 21:59
 */
public abstract class ConfigModelUtil {

    /**
     * 将配置模型转换为存储层入参(按表拆分列 + json)。
     *
     * @param model 配置模型
     * @return 存储层入参
     */
    public static Map<String, Object> convertModelToMap(ConfigModel model) {
        // Meta：严格按 dbsyncer_meta 拆分列，无 name/type/json
        if (model instanceof Meta) {
            return convertMetaToMap((Meta) model);
        }

        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, model.getId());
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, model.getType());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, model.getName());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, model.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, model.getUpdateTime());
        params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(model));

        // 表映射关系：关联信息落拆分列
        if (model instanceof TableGroup) {
            TableGroup tableGroup = (TableGroup) model;
            params.put(ConfigConstant.TABLE_GROUP_TASK_ID, tableGroup.getTaskId());
            params.put(ConfigConstant.TABLE_GROUP_SORT_INDEX, tableGroup.getIndex());
            params.put(ConfigConstant.TABLE_GROUP_SOURCE_CONNECTOR_ID, StringUtil.getIfBlank(tableGroup.getSourceConnectorId(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_TARGET_CONNECTOR_ID, StringUtil.getIfBlank(tableGroup.getTargetConnectorId(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_SOURCE_DATABASE, StringUtil.getIfBlank(tableGroup.getSourceDatabase(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_TARGET_DATABASE, StringUtil.getIfBlank(tableGroup.getTargetDatabase(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_SOURCE_SCHEMA, StringUtil.getIfBlank(tableGroup.getSourceSchema(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_TARGET_SCHEMA, StringUtil.getIfBlank(tableGroup.getTargetSchema(), StringUtil.EMPTY));
            params.put(ConfigConstant.TABLE_GROUP_SOURCE_TABLE, tableName(tableGroup.getSourceTable()));
            params.put(ConfigConstant.TABLE_GROUP_TARGET_TABLE, tableName(tableGroup.getTargetTable()));
            params.put(ConfigConstant.TABLE_GROUP_SOURCE_TOTAL, tableGroup.getSourceTotal());
            params.put(ConfigConstant.TABLE_GROUP_TARGET_TOTAL, tableGroup.getTargetTotal());
        }
        return params;
    }

    /**
     * 单条用户行转存储入参(dbsyncer_user 拆分列，无 name/type/json)。
     */
    public static Map<String, Object> convertUserInfoToMap(UserInfo user) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, user.getId());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, user.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, user.getUpdateTime());
        params.put(ConfigConstant.USER_USERNAME, user.getUsername());
        params.put(ConfigConstant.USER_PASSWORD, user.getPassword());
        params.put(ConfigConstant.USER_NICKNAME, user.getNickname());
        params.put(ConfigConstant.USER_ROLE, user.getRoleCode());
        params.put(ConfigConstant.USER_EMAIL, StringUtil.getIfBlank(user.getEmail(), StringUtil.EMPTY));
        params.put(ConfigConstant.USER_PHONE, StringUtil.getIfBlank(user.getPhone(), StringUtil.EMPTY));
        return params;
    }

    /**
     * 从存储行还原模型（与 {@link #convertModelToMap}/{@link #convertUserInfoToMap} 对称）。
     */
    public static <T> T parseFromRow(Map row, Class<T> clazz) {
        if (row == null || clazz == null) {
            return null;
        }
        if (Meta.class.equals(clazz)) {
            return (T) parseMeta(row);
        }
        if (UserInfo.class.equals(clazz)) {
            return (T) parseUserInfo(row);
        }
        Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
        if (json == null) {
            return null;
        }
        return JsonUtil.jsonToObj(String.valueOf(json), clazz);
    }

    private static Meta parseMeta(Map row) {
        Map<String, Object> data = new HashMap<>(row);
        Object snapshot = data.get(ConfigConstant.META_SNAPSHOT);
        if (snapshot instanceof String && StringUtil.isNotBlank((String) snapshot)) {
            data.put(ConfigConstant.META_SNAPSHOT, JsonUtil.parseMap((String) snapshot));
        }
        Meta meta = JsonUtil.mapToObj(data, Meta.class);
        return meta == null ? new Meta() : meta;
    }

    private static UserInfo parseUserInfo(Map row) {
        HashMap data = new HashMap<>(row);
        // 列名 role 对应模型字段 roleCode
        Object role = data.remove(ConfigConstant.USER_ROLE);
        if (role != null) {
            data.put("roleCode", role);
        }
        return JsonUtil.mapToObj(data, UserInfo.class);
    }

    private static Map<String, Object> convertMetaToMap(Meta meta) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, meta.getId());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, meta.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, meta.getUpdateTime());
        params.put(ConfigConstant.META_TASK_ID, meta.getTaskId());
        params.put(ConfigConstant.META_STATE, meta.getState());
        params.put(ConfigConstant.META_IS_TASK_DETAIL, meta.getIsTaskDetail());
        params.put(ConfigConstant.META_TOTAL, meta.getTotal() == null ? 0L : meta.getTotal().get());
        params.put(ConfigConstant.META_SUCCESS, meta.getSuccess() == null ? 0L : meta.getSuccess().get());
        params.put(ConfigConstant.META_FAIL, meta.getFail() == null ? 0L : meta.getFail().get());
        params.put(ConfigConstant.META_DIFF, meta.getDiff() == null ? 0L : meta.getDiff().get());
        params.put(ConfigConstant.META_FIXED, meta.getFixed() == null ? 0L : meta.getFixed().get());
        params.put(ConfigConstant.META_SNAPSHOT, JsonUtil.objToJson(meta.getSnapshot() == null ? new HashMap<>() : meta.getSnapshot()));
        return params;
    }

    private static String tableName(Table table) {
        return table == null || StringUtil.isBlank(table.getName()) ? StringUtil.EMPTY : table.getName();
    }

    /**
     * 依据配置类型路由到对应的存储表。
     * <p>同步 Mapping 已并入 dbsyncer_task，{@link ConfigConstant#MAPPING} 路由到 {@link StorageEnum#TASK}。
     *
     * @param type 配置类型 {@link ConfigConstant}
     * @return 存储枚举
     */
    public static StorageEnum getStorageEnum(String type) {
        if (type == null) {
            return StorageEnum.CONFIG;
        }
        switch (type) {
            case ConfigConstant.USER:
                return StorageEnum.USER;
            case ConfigConstant.CONNECTOR:
                return StorageEnum.CONNECTOR;
            case ConfigConstant.MAPPING:
            case ConfigConstant.TASK:
                // 同步/校验/迁移统一存 dbsyncer_task
                return StorageEnum.TASK;
            case ConfigConstant.TABLE_GROUP:
                return StorageEnum.TABLE_GROUP;
            case ConfigConstant.META:
                return StorageEnum.META;
            default:
                // VALIDATE_SYNC / DATABASE_SYNC 等企业任务 type 也走 task 表
                if (type.contains("SYNC") || type.contains("TASK")) {
                    return StorageEnum.TASK;
                }
                return StorageEnum.CONFIG;
        }
    }
}
