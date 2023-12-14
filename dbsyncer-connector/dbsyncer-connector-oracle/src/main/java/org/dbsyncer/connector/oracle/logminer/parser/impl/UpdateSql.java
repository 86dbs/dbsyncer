/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser.impl;

import net.sf.jsqlparser.statement.update.Update;
import org.dbsyncer.connector.oracle.logminer.parser.AbstractParser;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-14 14:58
 */
public class UpdateSql extends AbstractParser {

    private Update update;

    public UpdateSql(Update update) {
        this.update = update;
    }

    @Override
    public List<Object> parseColumns() {
        findColumn(update.getWhere());
        return getColumnsFromDB();
    }

}
