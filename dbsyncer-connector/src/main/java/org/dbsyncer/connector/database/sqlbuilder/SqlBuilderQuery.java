package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderQuery implements SqlBuilder {

    @Override
    public String buildSql(String tableName, String pk, List<String> filedNames, String queryFilter, String quotation, Database database) {
        // 分页语句
        return database.getPageSql(tableName, pk, buildStandardSql(tableName, pk, filedNames, queryFilter, quotation));
    }

    public String buildStandardSql(String tableName, String pk, List<String> filedNames, String queryFilter, String quotation) {
        StringBuilder sql = new StringBuilder();
        int size = filedNames.size();
        int end = size - 1;
        for (int i = 0; i < size; i++) {
            // "USERNAME"
            sql.append(quotation).append(filedNames.get(i)).append(quotation);
            //如果不是最后一个字段
            if (i < end) {
                sql.append(", ");
            }
        }
        // SELECT "ID","NAME" FROM "USER"
        sql.insert(0, "SELECT ").append(" FROM ").append(quotation).append(tableName).append(quotation);
        // 解析查询条件
        if (StringUtils.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        return sql.toString();
    }

}