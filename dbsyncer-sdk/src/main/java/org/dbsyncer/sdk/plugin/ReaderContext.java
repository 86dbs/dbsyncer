/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.workitem.WorkBound;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:28
 */
public interface ReaderContext extends BaseContext {

    boolean isSupportedCursor();

    List<Object> getArgs();

    Object[] getCursors();

    int getPageIndex();

    int getPageSize();

    default String getCommandKey(){
        return StringUtil.EMPTY;
    }

    /**
     * 当前工作项边界；整表或未设置为 null。
     *
     * @return 工作项边界
     */
    default WorkBound getWorkBound() {
        return null;
    }
}
