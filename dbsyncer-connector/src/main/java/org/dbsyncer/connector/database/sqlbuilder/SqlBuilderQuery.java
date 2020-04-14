package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderQuery implements SqlBuilder {

    @Override
    public String buildSql(String tableName, String pk, List<String> filedNames, String queryFilter, Database database) {
        StringBuilder sql = new StringBuilder();
        int size = filedNames.size();
        int end = size - 1;
        for (int i = 0; i < size; i++) {
            // USER.USERNAME
            sql.append(tableName).append(".").append(filedNames.get(i));
            //如果不是最后一个字段
            if (i < end) {
                sql.append(", ");
            }
        }
        // SELECT USER.ID,USER.NAME FROM USER
        sql.insert(0, "SELECT ").append(" FROM ").append(tableName);
        // 解析查询条件
        if (StringUtils.isNotBlank(queryFilter)) {
            sql.append(queryFilter);
        }
        // 分页语句
        return database.getPageSql(tableName, pk, sql.toString());
    }

}