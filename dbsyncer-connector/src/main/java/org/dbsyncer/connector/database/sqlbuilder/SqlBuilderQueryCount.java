package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/5/2 0:03
 */
public class SqlBuilderQueryCount implements SqlBuilder {

    @Override
    public String buildSql(String tableName, String pk, List<String> filedNames, String queryFilter, Database database) {
        StringBuilder sql = new StringBuilder();
        // SELECT COUNT(*) FROM USER
        sql.append("select count(*) from ").append(tableName);
        // 解析查询条件
        if (StringUtils.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        return sql.toString();
    }
}