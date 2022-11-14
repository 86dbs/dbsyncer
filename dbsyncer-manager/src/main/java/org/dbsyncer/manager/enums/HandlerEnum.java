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
    OPR_ADD("add", new Handler<OperationCallBack>() {
        @Override
        public Object execute(OperationCallBack operationCallBack) {
            return operationCallBack.add();
        }
    }),

    /**
     * 修改
     */
    OPR_EDIT("edit", new Handler<OperationCallBack>() {
        @Override
        public Object execute(OperationCallBack operationCallBack) {
            return operationCallBack.edit();
        }
    }),

    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(ConfigConstant.CONNECTOR, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseConnector();
        }
    }),

    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(ConfigConstant.MAPPING, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMapping();
        }
    }),

    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(ConfigConstant.TABLE_GROUP, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseTableGroup();
        }
    }, GroupStrategyEnum.TABLE),

    /**
     * 预加载Meta
     */
    PRELOAD_META(ConfigConstant.META, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMeta();
        }
    }),

    /**
     * 预加载Config
     */
    PRELOAD_CONFIG(ConfigConstant.CONFIG, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseConfig();
        }
    }),

    /**
     * 预加载ProjectGroup
     */
    PRELOAD_PROJECT_GROUP(ConfigConstant.PROJECT_GROUP, true, new Handler<PreloadCallBack>() {
        @Override
        public Object execute(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseProjectGroup();
        }
    });

    private String modelType;
    private boolean preload;
    private Handler handler;
    private GroupStrategyEnum groupStrategyEnum;

    HandlerEnum(String modelType, Handler handler) {
        this(modelType, false, handler);
    }

    HandlerEnum(String modelType, boolean preload, Handler handler) {
        this(modelType, preload, handler, GroupStrategyEnum.DEFAULT);
    }

    HandlerEnum(String modelType, boolean preload, Handler handler, GroupStrategyEnum groupStrategyEnum) {
        this.modelType = modelType;
        this.preload = preload;
        this.handler = handler;
        this.groupStrategyEnum = groupStrategyEnum;
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