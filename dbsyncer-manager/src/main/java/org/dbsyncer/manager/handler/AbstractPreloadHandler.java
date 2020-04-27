package org.dbsyncer.manager.handler;

import org.dbsyncer.manager.config.PreloadCallBack;
import org.dbsyncer.manager.template.Callback;
import org.dbsyncer.manager.template.Handler;

public abstract class AbstractPreloadHandler implements Handler {

    /**
     * 交给实现handler处理
     *
     * @param preloadCallBack
     * @return
     */
    protected abstract Object preload(PreloadCallBack preloadCallBack);

    @Override
    public Object execute(Callback call) {
        if (null != call && call instanceof PreloadCallBack) {
            PreloadCallBack preloadCallBack = (PreloadCallBack) call;
            return preload(preloadCallBack);
        }
        return null;
    }

}