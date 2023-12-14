package org.dbsyncer.connector.oracle.logminer.parser;

import java.util.List;

public interface Parser {

    List<Object> parseSql();
}
