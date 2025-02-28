/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.JSQLParserException;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.spi.ConnectorService;

public interface DDLParser {

    DDLConfig parse(ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException;

    void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig);
}