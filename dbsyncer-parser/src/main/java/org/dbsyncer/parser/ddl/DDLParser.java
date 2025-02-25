/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.spi.ConnectorService;

public interface DDLParser {

    DDLConfig parseDDlConfig(ConnectorService connectorService, TableGroup tableGroup, String sql);

    void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig);
}