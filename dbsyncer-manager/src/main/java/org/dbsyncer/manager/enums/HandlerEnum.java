package org.dbsyncer.manager.enums;

import org.dbsyncer.manager.config.OperationCallBack;
import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.template.Handler;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum HandlerEnum {

    /**
     * 添加
     */
    OPR_ADD("add", (callback) -> ((OperationCallBack) callback).add()),

    /**
     * 修改
     */
    OPR_EDIT("edit", (callback) -> ((OperationCallBack) callback).edit()),

    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(ConfigConstant.CONNECTOR, true, (callback) -> ((PreloadCallBack) callback).parseConnector()),

    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(ConfigConstant.MAPPING, true, (callback) -> ((PreloadCallBack) callback).parseMapping()),

    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(ConfigConstant.TABLE_GROUP, true, GroupStrategyEnum.TABLE, (callback) -> ((PreloadCallBack) callback).parseTableGroup()),

    /**
     * 预加载Meta
     */
    PRELOAD_META(ConfigConstant.META, true, (callback) -> ((PreloadCallBack) callback).parseMeta()),

    /**
     * 预加载Config
     */
    PRELOAD_CONFIG(ConfigConstant.CONFIG, true, (callback) -> ((PreloadCallBack) callback).parseConfig()),

    /**
     * 预加载ProjectGroup
     */
    PRELOAD_PROJECT_GROUP(ConfigConstant.PROJECT_GROUP, true, (callback) -> ((PreloadCallBack) callback).parseProjectGroup());

    private String modelType;
    private boolean preload;
    private Handler handler;
    private GroupStrategyEnum groupStrategyEnum;

    HandlerEnum(String modelType, Handler handler) {
        this(modelType, false, handler);
    }

    HandlerEnum(String modelType, boolean preload, Handler handler) {
        this(modelType, preload, GroupStrategyEnum.DEFAULT, handler);
    }

    HandlerEnum(String modelType, boolean preload, GroupStrategyEnum groupStrategyEnum, Handler handler) {
        this.modelType = modelType;
        this.preload = preload;
        this.groupStrategyEnum = groupStrategyEnum;
        this.handler = handler;
    }

    public String getModelType() {
        return modelType;
    }

    public boolean isPreload() {
        return preload;
    }

    public Handler getHandler() {
        return handler;
    }

    public GroupStrategyEnum getGroupStrategyEnum() {
        return groupStrategyEnum;
    }
}