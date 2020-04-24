package org.dbsyncer.manager.enums;

import org.dbsyncer.manager.config.OperationCallBack;
import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.handler.AbstractOperationHandler;
import org.dbsyncer.manager.handler.AbstractPreloadHandler;
import org.dbsyncer.manager.template.Handler;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum HandlerEnum {

    /**
     * 添加
     */
    OPR_ADD(new AbstractOperationHandler(){
        @Override
        protected void handle(OperationCallBack operationCallBack) {
            operationCallBack.add();
        }
    }),
    /**
     * 修改
     */
    OPR_EDIT(new AbstractOperationHandler(){
        @Override
        protected void handle(OperationCallBack operationCallBack) {
            operationCallBack.edit();
        }
    }),
    /**
     * 预加载Connector
     */
    PRELOAD_CONNECTOR(new AbstractPreloadHandler(){
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseConnector();
        }
    }),
    /**
     * 预加载Mapping
     */
    PRELOAD_MAPPING(new AbstractPreloadHandler(){
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMapping();
        }
    }),
    /**
     * 预加载TableGroup
     */
    PRELOAD_TABLE_GROUP(new AbstractPreloadHandler(){
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseTableGroup();
        }
    }),
    /**
     * 预加载Meta
     */
    PRELOAD_META(new AbstractPreloadHandler(){
        @Override
        protected Object preload(PreloadCallBack preloadCallBack) {
            return preloadCallBack.parseMeta();
        }
    });

    private Handler handler;

    HandlerEnum(Handler handler) {
        this.handler = handler;
    }

    public Handler getHandler() {
        return handler;
    }
}
