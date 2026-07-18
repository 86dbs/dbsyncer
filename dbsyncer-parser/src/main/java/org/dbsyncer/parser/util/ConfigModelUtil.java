package org.dbsyncer.parser.util;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/19 21:59
 */
public abstract class ConfigModelUtil {

    /**
     * 将配置模型转换为存储层入参(通用列 + 各类型拆分列 + json)。
     *
     * @param model 配置模型
     * @return 存储层入参
     */
    public static Map<String, Object> convertModelToMap(ConfigModel model) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, model.getId());
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, model.getType());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, model.getName());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, model.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, model.getUpdateTime());
        params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(model));

        // 表映射关系：拆分 mappingId/sortIndex
        if (model instanceof TableGroup) {
            TableGroup tableGroup = (TableGroup) model;
            params.put(ConfigConstant.TABLE_GROUP_MAPPING_ID, tableGroup.getMappingId());
            params.put(ConfigConstant.TABLE_GROUP_SORT_INDEX, tableGroup.getIndex());
            return params;
        }

        // 任务执行结果：拆分 state/total/success/fail 计数列
        if (model instanceof Meta) {
            Meta meta = (Meta) model;
            params.put(ConfigConstant.META_STATE, meta.getState());
            params.put(ConfigConstant.META_TOTAL, meta.getTotal() == null ? 0L : meta.getTotal().get());
            params.put(ConfigConstant.META_SUCCESS, meta.getSuccess() == null ? 0L : meta.getSuccess().get());
            params.put(ConfigConstant.META_FAIL, meta.getFail() == null ? 0L : meta.getFail().get());
        }
        return params;
    }

    /**
     * 依据配置类型路由到对应的存储表。
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
                return StorageEnum.MAPPING;
            case ConfigConstant.TABLE_GROUP:
                return StorageEnum.TABLE_GROUP;
            case ConfigConstant.META:
                return StorageEnum.META;
            default:
                // system/notice 等全局配置
                return StorageEnum.CONFIG;
        }
    }
}
