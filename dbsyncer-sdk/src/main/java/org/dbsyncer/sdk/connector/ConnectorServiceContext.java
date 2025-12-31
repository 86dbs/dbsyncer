/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.model.Table;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-10-25 16:32
 */
public interface ConnectorServiceContext {

    String getCatalog();

    String getSchema();

    List<String> getTablePatterns();

    List<Table> getSqlPatterns();
}