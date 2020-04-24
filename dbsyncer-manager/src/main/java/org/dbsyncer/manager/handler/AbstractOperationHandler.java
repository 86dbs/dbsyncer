package org.dbsyncer.manager.handler;

import org.dbsyncer.manager.config.OperationCallBack;
import org.dbsyncer.manager.template.Callback;
import org.dbsyncer.manager.template.Handler;

public abstract class AbstractOperationHandler implements Handler {

    /**
     * 交给实现handler处理
     *
     * @param operationCallBack
     * @return
     */
    protected abstract void handle(OperationCallBack operationCallBack);

    @Override
    public Object execute(Callback call) {
        if (null != call && call instanceof OperationCallBack) {
            handle((OperationCallBack) call);
        }
        return null;
    }

}