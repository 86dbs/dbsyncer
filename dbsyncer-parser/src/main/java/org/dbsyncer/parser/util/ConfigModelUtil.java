package org.dbsyncer.parser.util;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.GroupRel;
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
        // GroupRel：严格按 dbsyncer_group_rel 拆分列，无 name/json；TYPE 存资源类型
        if (model instanceof GroupRel) {
            return convertGroupRelToMap((GroupRel) model);
        }
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, model.getId());
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, model.getType());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, model.getName());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, model.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, model.getUpdateTime());
        params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(model));

        // 连接器：源/目标角色落拆分列（1/0），便于 search 按角色过滤
        if (model instanceof Connector) {
            Connector connector = (Connector) model;
            params.put(ConfigConstant.CONNECTOR_IS_SOURCE, connector.isSource() ? 1 : 0);
            params.put(ConfigConstant.CONNECTOR_IS_TARGET, connector.isTarget() ? 1 : 0);
            params.put(ConfigConstant.CONNECTOR_STATUS, connector.getStatus());
        }

        // 业务分组：排序/归属落拆分列（表无 type 列，params 中 type 仅路由用会被忽略）
        if (model instanceof Group) {
            Group group = (Group) model;
            params.put(ConfigConstant.GROUP_SORT, group.getSort());
            params.put(ConfigConstant.GROUP_OWNER_ID, StringUtil.getIfBlank(group.getOwnerId(), StringUtil.EMPTY));
            params.put(ConfigConstant.GROUP_OWNER_ROLE, StringUtil.getIfBlank(group.getOwnerRole(), StringUtil.EMPTY));
        }

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
        if (GroupRel.class.equals(clazz)) {
            return (T) parseGroupRel(row);
        }
        Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
        if (json == null) {
            return null;
        }
        return JsonUtil.jsonToObj(String.valueOf(json), clazz);
    }

    /**
     * 分组关系行转模型（TYPE 列直接映射父类 type）。
     */
    private static GroupRel parseGroupRel(Map row) {
        return JsonUtil.mapToObj(row, GroupRel.class);
    }

    /**
     * 分组关系转存储入参：TYPE 列写父类 type（资源类型），不写 name/json。
     */
    private static Map<String, Object> convertGroupRelToMap(GroupRel rel) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, rel.getId());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, rel.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, rel.getUpdateTime());
        params.put(ConfigConstant.GROUP_REL_GROUP_ID, StringUtil.getIfBlank(rel.getGroupId(), StringUtil.EMPTY));
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, StringUtil.getIfBlank(rel.getType(), StringUtil.EMPTY));
        params.put(ConfigConstant.GROUP_REL_RESOURCE_ID, StringUtil.getIfBlank(rel.getResourceId(), StringUtil.EMPTY));
        return params;
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
        params.put(ConfigConstant.META_START_TIME, meta.getStartTime());
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
     * 按模型实例路由存储表。{@link GroupRel#getType()} 存资源类型，不能按 type 字符串路由。
     */
    public static StorageEnum getStorageEnum(ConfigModel model) {
        if (model instanceof GroupRel) {
            return StorageEnum.GROUP_REL;
        }
        return getStorageEnum(model == null ? null : model.getType());
    }

    /**
     * 按模型 Class 路由存储表。
     */
    public static StorageEnum getStorageEnum(Class<?> clazz) {
        if (clazz != null && GroupRel.class.isAssignableFrom(clazz)) {
            return StorageEnum.GROUP_REL;
        }
        if (clazz == null || !ConfigModel.class.isAssignableFrom(clazz)) {
            return StorageEnum.CONFIG;
        }
        try {
            ConfigModel probe = (ConfigModel) clazz.newInstance();
            return getStorageEnum(probe.getType());
        } catch (Exception e) {
            return StorageEnum.CONFIG;
        }
    }

    /**
     * 依据配置类型路由到对应的存储表。
     * <p>同步 Mapping 与企业任务（validateSync / databaseSync）均并入 {@code dbsyncer_task}。
     *
     * @param type 配置类型 {@link ConfigConstant} / {@link CommonTaskTypeEnum#name()}
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
            case ConfigConstant.VALIDATE_SYNC:
            case ConfigConstant.DATABASE_SYNC:
                // 同步/校验/迁移统一存 dbsyncer_task
                return StorageEnum.TASK;
            case ConfigConstant.TABLE_GROUP:
                return StorageEnum.TABLE_GROUP;
            case ConfigConstant.META:
                return StorageEnum.META;
            case ConfigConstant.GROUP:
                return StorageEnum.GROUP;
            case ConfigConstant.GROUP_REL:
                return StorageEnum.GROUP_REL;
            default:
                // 后续新增的 CommonTaskTypeEnum 仍走 task 表
                return CommonTaskTypeEnum.parse(type) != null ? StorageEnum.TASK : StorageEnum.CONFIG;
        }
    }
}
