/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.cdc.OracleListener;
import org.dbsyncer.connector.oracle.schema.OracleClobValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleOtherValueMapper;
import org.dbsyncer.connector.oracle.validator.OracleConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.List;

/**
 * Oracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public final class OracleConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final OracleConfigValidator configValidator = new OracleConfigValidator();

    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
        VALUE_MAPPERS.put(Types.CLOB, new OracleClobValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "Oracle";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new OracleListener();
        }
        return null;
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        // 如果不支持游标，则没有WHERE条件和ORDER BY，只做简单的ROWNUM限制
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            // 不支持游标分页的情况：只做简单的ROWNUM分页（性能较差，建议使用支持的主键类型）
            logger.warn("表不支持游标分页，主键类型必须是可比较类型（数字、字符、日期时间等），将使用简单的ROWNUM分页");
            StringBuilder sql = new StringBuilder();
            sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
            sql.append(config.getQuerySql());
            sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
            return sql.toString();
        }

        // 支持游标分页：使用与getPageCursorSql相同的逻辑（但第一次查询时无游标条件）
        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        
        final String quotation = buildSqlWithQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        
        // 第一次查询时没有游标条件，直接添加ORDER BY
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder();
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_START);
        sql.append(config.getQuerySql());
        boolean skipFirst = false;
        // 没有过滤条件
        if (StringUtil.isBlank(config.getQueryFilter())) {
            skipFirst = true;
            sql.append(" WHERE ");
        }
        final String quotation = buildSqlWithQuotation();
        final List<String> primaryKeys = config.getPrimaryKeys();
        
        // 构建复合键的游标分页条件: (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR ...
        buildCursorCondition(sql, primaryKeys, quotation, skipFirst);
        
        // 添加 ORDER BY - 必须按主键排序以保证游标分页的稳定性
        sql.append(" ORDER BY ");
        PrimaryKeyUtil.buildSql(sql, primaryKeys, quotation, ",", "", true);
        
        sql.append(DatabaseConstant.ORACLE_PAGE_SQL_END);
        return sql.toString();
    }
    
    /**
     * 构建复合键的游标分页条件（支持任意数量的主键）
     * 
     * <p>对于单字段主键 (pk1)：生成条件：pk1 > ?</p>
     * <p>对于2字段主键 (pk1, pk2)：生成条件：(pk1 > ?) OR (pk1 = ? AND pk2 > ?)</p>
     * <p>对于3字段主键 (pk1, pk2, pk3)：生成条件：(pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?)</p>
     * <p>对于n字段主键：依此类推...</p>
     * 
     * <p>原理：对于复合键 (pk1, pk2, ..., pkn)，要查询所有大于 (v1, v2, ..., vn) 的记录，
     * 需要满足：(pk1 > v1) OR (pk1 = v1 AND pk2 > v2) OR ... OR (pk1 = v1 AND pk2 = v2 AND ... AND pkn > vn)</p>
     * 
     * @param sql SQL构建器
     * @param primaryKeys 主键列表（支持任意长度）
     * @param quotation 引号
     * @param skipFirst 是否跳过第一个连接符
     */
    private void buildCursorCondition(StringBuilder sql, List<String> primaryKeys, String quotation, boolean skipFirst) {
        if (primaryKeys.isEmpty()) {
            return;
        }
        
        // 单字段主键：直接使用 pk > ?
        if (primaryKeys.size() == 1) {
            if (!skipFirst) {
                sql.append(" AND ");
            }
            sql.append(quotation).append(primaryKeys.get(0)).append(quotation).append(" > ? ");
            return;
        }
        
        // 复合键：构建 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR ...
        if (!skipFirst) {
            sql.append(" AND ");
        }
        sql.append("(");
        
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append("(");
            
            // 前面的字段都是等号
            for (int j = 0; j < i; j++) {
                if (j > 0) {
                    sql.append(" AND ");
                }
                sql.append(quotation).append(primaryKeys.get(j)).append(quotation).append(" = ? ");
            }
            
            // 最后一个字段使用大于号
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(quotation).append(primaryKeys.get(i)).append(quotation).append(" > ? ");
            sql.append(")");
        }
        
        sql.append(") ");
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        // 统一使用游标分页方式：第一次查询时无游标，只需要pageSize参数
        int pageSize = context.getPageSize();
        return new Object[]{pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        // 第一次查询（无游标）
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize};
        }
        // 单字段主键：只需要 pk > ?，参数为 [last_pk, pageSize]
        int pkCount = cursors.length;
        if (pkCount == 1) {
            Object[] newCursors = new Object[2];
            newCursors[0] = cursors[0];
            newCursors[1] = pageSize;
            return newCursors;
        }
        
        // 复合键（支持任意数量的主键）
        // WHERE 条件为 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?) OR ...
        // 
        // 参数数量计算：等差数列求和
        // - 1个主键：1个参数 (pk1 > ?)
        // - 2个主键：1+2=3个参数 (pk1 > ?) OR (pk1 = ? AND pk2 > ?)
        // - 3个主键：1+2+3=6个参数 (pk1 > ?) OR (pk1 = ? AND pk2 > ?) OR (pk1 = ? AND pk2 = ? AND pk3 > ?)
        // - n个主键：1+2+...+n = n*(n+1)/2 个参数
        //
        // 参数顺序示例（2个主键，cursors=[v1, v2]）：
        //   [v1, v1, v2, pageSize] 对应条件 (pk1 > v1) OR (pk1 = v1 AND pk2 > v2) 限制 ROWNUM <= pageSize
        // 参数顺序示例（3个主键，cursors=[v1, v2, v3]）：
        //   [v1, v1, v2, v1, v2, v3, pageSize] 对应条件 (pk1 > v1) OR (pk1 = v1 AND pk2 > v2) OR (pk1 = v1 AND pk2 = v2 AND pk3 > v3) 限制 ROWNUM <= pageSize
        int paramCount = pkCount * (pkCount + 1) / 2;
        Object[] newCursors = new Object[paramCount + 1];
        
        int index = 0;
        // 遍历每个主键字段，生成对应的参数
        for (int i = 0; i < pkCount; i++) {
            // 对于第 i 个主键，前面的所有主键都需要等号条件
            for (int j = 0; j < i; j++) {
                newCursors[index++] = cursors[j];
            }
            // 第 i 个主键使用大于号条件
            newCursors[index++] = cursors[i];
        }
        
        newCursors[paramCount] = pageSize;
        return newCursors;
    }

    @Override
    public boolean enableCursor() {
        return true;
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }

}