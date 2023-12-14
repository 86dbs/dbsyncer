/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser.impl;

import net.sf.jsqlparser.statement.delete.Delete;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.logminer.parser.AbstractParser;
import org.dbsyncer.sdk.model.Field;

import java.math.BigInteger;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

/**
 * @Author life
 * @Version 1.0.0
 * @Date 2023-12-14 14:58
 */
public class DeleteSql extends AbstractParser {

    private Delete delete;

    public DeleteSql(Delete delete) {
        this.delete = delete;
    }

    @Override
    public List<Object> parseColumns() {
        findColumn(delete.getWhere());
        List<Object> data = new LinkedList<>();
        for (Field field : fields) {
            if (field.isPk()) {
                Object value = columnMap.get(field.getName());
                switch (field.getType()) {
                    case Types.DECIMAL:
                        value = new BigInteger(StringUtil.replace(value.toString(), StringUtil.SINGLE_QUOTATION, StringUtil.EMPTY));
                        break;
                }
                data.add(value);
            } else {
                data.add(null);
            }
        }
        return data;
    }

}
