/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

import java.util.List;

public interface Parser {

    List<Object> parseColumns();
}
