package org.dbsyncer.manager.enums;

import org.dbsyncer.manager.config.OperationCallBack;
import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.handler.AbstractOperationHandler;
import org.dbsyncer.manager.handler.AbstractPreloadHandler;
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
    OPR_ADD("add", new AbstractOperationHandler() {
        @Override
        protected void handle(OperationCallBack operationCallBack) {
            operationCallBack.add();
        }
    }),

    /**
     * 修改
     */
    OPR_EDIT("edit", new AbstractOperationHandler() {
        @Override
        protected void handle(OperationCallBack operationCallBack) {
            operationCallBack.edit();
        }
    }),

    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(ConfigConstant.CONNECTOR, true, new AbstractPreloadHandler() {
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseConnector();
        }
    }),

    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(ConfigConstant.MAPPING, true, new AbstractPreloadHandler() {
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMapping();
        }
    }),

    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(ConfigConstant.TABLE_GROUP, true, new AbstractPreloadHandler() {
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseTableGroup();
        }
    }, GroupStrategyEnum.TABLE),

    /**
     * 预加载Meta
     */
    PRELOAD_META(ConfigConstant.META, true, new AbstractPreloadHandler() {
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMeta();
        }
    }),

    /**
     * 预加载Config
     */
    PRELOAD_CONFIG(ConfigConstant.CONFIG, true, new AbstractPreloadHandler() {
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseConfig();
        }
    });

    private String modelType;
    private boolean preload;
    private Handler handler;
    private GroupStrategyEnum groupStrategyEnum;

    HandlerEnum(String modelType, Handler handler) {
        this(modelType, false, handler, null);
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