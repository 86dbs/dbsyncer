/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.model.Table;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-12 00:47
 */
public interface MetaContext extends BaseContext {

    Table getSourceTable();

}
