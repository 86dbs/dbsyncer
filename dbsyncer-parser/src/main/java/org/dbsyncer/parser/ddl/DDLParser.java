/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.spi.ConnectorService;

import net.sf.jsqlparser.JSQLParserException;

import net.sf.jsqlparser.JSQLParserException;

public interface DDLParser {

    DDLConfig parse(ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException;

    void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig);
}
