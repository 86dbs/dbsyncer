/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.model.ValidateSyncTask;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-01-05 22:56
 */
public abstract class ConnectorServiceContextUtil {

    public static DefaultConnectorServiceContext buildConnectorServiceContext(Mapping mapping, boolean isSource) {
        return buildConnectorServiceContext(mapping.getId(), mapping.getSourceConnectorId(), mapping.getSourceDatabase(), mapping.getSourceSchema(),
                mapping.getTargetConnectorId(), mapping.getTargetDatabase(), mapping.getTargetSchema(), isSource);
    }

    public static DefaultConnectorServiceContext buildConnectorServiceContext(ValidateSyncTask task, boolean isSource) {
        return buildConnectorServiceContext(task.getId(), task.getSourceConnectorId(), task.getSourceDatabase(), task.getSourceSchema(),
                task.getTargetConnectorId(), task.getTargetDatabase(), task.getTargetSchema(), isSource);
    }

    public static DefaultConnectorServiceContext buildConnectorServiceContext(String uniqueId, String sourceConnectorId, String sourceDatabase, String sourceSchema,
                                                                              String targetConnectorId, String targetDatabase, String targetSchema, boolean isSource) {
        DefaultConnectorServiceContext context = new DefaultConnectorServiceContext();
        context.setMappingId(uniqueId);
        context.setCatalog(isSource ? sourceDatabase : targetDatabase);
        context.setSchema(isSource ? sourceSchema : targetSchema);
        context.setConnectorId(isSource ? sourceConnectorId : targetConnectorId);
        context.setSuffix(isSource ? ConnectorInstanceUtil.SOURCE_SUFFIX : ConnectorInstanceUtil.TARGET_SUFFIX);
        return context;
    }
}
