/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser.impl;

import net.sf.jsqlparser.statement.delete.Delete;
import org.dbsyncer.connector.oracle.logminer.parser.AbstractParser;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @Author life
 * @Version 1.0.0
 * @Date 2023-12-14 14:58
 */
public class DeleteSql extends AbstractParser {

    private Delete delete;

    public DeleteSql(Delete delete, List<Field> fields) {
        this.delete = delete;
        setFields(fields);
    }

    @Override
    public List<Object> parseColumns() {
        findColumn(delete.getWhere());
        return columnMapToData();
    }

}