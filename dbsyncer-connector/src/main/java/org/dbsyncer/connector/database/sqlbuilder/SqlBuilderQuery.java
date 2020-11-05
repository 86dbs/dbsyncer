package org.dbsyncer.connector.database.sqlbuilder;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.SqlBuilderConfig;
import org.dbsyncer.connector.database.AbstractSqlBuilder;
import org.dbsyncer.connector.database.Database;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/27 0:03
 */
public class SqlBuilderQuery extends AbstractSqlBuilder {

    @Override
    public String buildSql(SqlBuilderConfig config) {
        // 分页语句
        Database database = config.getDatabase();
        return database.getPageSql(config.getTableName(), config.getPk(), buildQuerySql(config));
    }

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        String tableName = config.getTableName();
        List<String> filedNames = config.getFiledNames();
        List<String> labelNames = config.getLabelNames();
        boolean appendLabel = !CollectionUtils.isEmpty(labelNames);
        String quotation = config.getQuotation();
        String queryFilter = config.getQueryFilter();

        StringBuilder sql = new StringBuilder();
        int size = filedNames.size();
        int end = size - 1;
        for (int i = 0; i < size; i++) {
            // "USERNAME"
            sql.append(quotation).append(filedNames.get(i)).append(quotation);

            // label
            if(appendLabel){
                // name as "myName"
                sql.append(" as \"").append(labelNames.get(i)).append("\"");
            }

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