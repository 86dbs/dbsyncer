/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser.impl;

import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.dbsyncer.common.util.StringUtil;
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
        passerSet(update.getUpdateSets());
        return columnMapToData();
    }

    private void passerSet(List<UpdateSet> updateSets){
        //解析替换
        for (UpdateSet updateSet:updateSets) {
            String columnName = StringUtil.replace(updateSet.getColumn(0).getColumnName(),
                    StringUtil.DOUBLE_QUOTATION,StringUtil.EMPTY);
            String value = parserValue(updateSet.getValue(0));
            columnMap.put(columnName,value);
        }
    }

}