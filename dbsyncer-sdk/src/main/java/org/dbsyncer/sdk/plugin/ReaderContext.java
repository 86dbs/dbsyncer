/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.model.Table;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:28
 */
public interface ReaderContext extends BaseContext {

    Table getSourceTable();

    List<Object> getArgs();

    Object[] getCursors();

    int getPageIndex();

    int getPageSize();

}