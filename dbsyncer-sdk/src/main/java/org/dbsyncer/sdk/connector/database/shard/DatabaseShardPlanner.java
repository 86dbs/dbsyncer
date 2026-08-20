/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.shard;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.shard.ShardPlan;
import org.dbsyncer.sdk.model.shard.ShardPlanRequest;
import org.dbsyncer.sdk.model.shard.ShardPlans;
import org.dbsyncer.sdk.model.shard.ShardSpec;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * 关系库切片规划：数值 PK → 小表整表、大表按 [MIN,MAX] 均分 RANGE（单片上限 60 万）；
 * 可下推时字符串/UUID → HASH_MOD；否则整表。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class DatabaseShardPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseShardPlanner.class);

    private DatabaseShardPlanner() {
    }

    /**
     * 规划切片。
     *
     * @param dialect 方言
     * @param db      连接
     * @param request 请求
     * @return 计划
     */
    public static ShardPlan plan(Database dialect, DatabaseConnectorInstance db, ShardPlanRequest request) {
        if (dialect == null || db == null || request == null || StringUtil.isBlank(request.getTableGroupId())) {
            return ShardPlan.wholeTable();
        }
        Table table = request.getSourceTable();
        Field pk = singlePk(table);
        if (pk == null) {
            return ShardPlan.wholeTable(request.getTableGroupId());
        }
        if (isNumericJdbcType(pk.getType())) {
            List<ShardSpec> ranges = planNumericRange(dialect, db, request, pk);
            if (!CollectionUtils.isEmpty(ranges) && ranges.size() > 1) {
                return ShardPlan.of(ranges);
            }
            return ShardPlan.wholeTable(request.getTableGroupId());
        }
        if (isStringJdbcType(pk.getType()) && dialect.supportsHashModShard()) {
            return ShardPlans.hashMod(request.getTableGroupId(), pk.getName(), request.suggestedHashMod());
        }
        return ShardPlan.wholeTable(request.getTableGroupId());
    }

    private static List<ShardSpec> planNumericRange(Database dialect, DatabaseConnectorInstance db,
                                                    ShardPlanRequest request, Field pk) {
        long[] bound = queryMinMax(dialect, db, request.getSchema(), request.getSourceTable().getName(), pk.getName());
        if (bound == null) {
            return null;
        }
        long maxChunk = Math.max(1L, request.maxNumericRangeChunk());
        long span = pkSpan(bound[0], bound[1]);
        // 小表：跨度不超过上限 → 整表，落到单一节点跑完
        if (span <= 0L || span <= maxChunk) {
            return null;
        }
        long chunk = computeRangeChunk(bound[0], bound[1], request.suggestedShardCount());
        // 大表：按 nodes*10 均分，但单片跨度不超过上限（必要时自动加片）
        chunk = Math.min(chunk, maxChunk);
        return NumericRangeSplitter.split(request.getTableGroupId(), pk.getName(), bound[0], bound[1], chunk);
    }

    /**
     * 按 [min,max] 均分到指定片数，chunk = ceil((max - min + 1) / shards)。
     */
    static long computeRangeChunk(long minPk, long maxPk, int shardCount) {
        int shards = Math.max(2, shardCount);
        long span = pkSpan(minPk, maxPk);
        if (span <= 0L) {
            return 1L;
        }
        if (span == Long.MAX_VALUE) {
            return Math.max(1L, Long.MAX_VALUE / shards);
        }
        return Math.max(1L, (span + shards - 1L) / shards);
    }

    /**
     * 主键闭区间跨度；溢出时返回 {@link Long#MAX_VALUE}。
     */
    static long pkSpan(long minPk, long maxPk) {
        if (maxPk < minPk) {
            return 0L;
        }
        if (minPk < 0L && maxPk >= 0L && maxPk > Long.MAX_VALUE + minPk) {
            return Long.MAX_VALUE;
        }
        long distance = maxPk - minPk;
        if (distance == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return distance + 1L;
    }

    /**
     * 单列主键字段。
     *
     * @param table 表
     * @return PK；复合或不存在为 null
     */
    public static Field singlePk(Table table) {
        if (table == null) {
            return null;
        }
        List<Field> pks = PrimaryKeyUtil.findPrimaryKeyFields(table.getColumn());
        if (CollectionUtils.isEmpty(pks) || pks.size() != 1) {
            return null;
        }
        return pks.get(0);
    }

    public static boolean isNumericJdbcType(int type) {
        return type == Types.BIGINT
                || type == Types.INTEGER
                || type == Types.SMALLINT
                || type == Types.TINYINT
                || type == Types.NUMERIC
                || type == Types.DECIMAL;
    }

    public static boolean isStringJdbcType(int type) {
        return type == Types.CHAR
                || type == Types.VARCHAR
                || type == Types.LONGVARCHAR
                || type == Types.NCHAR
                || type == Types.NVARCHAR
                || type == Types.LONGNVARCHAR
                || type == Types.OTHER;
    }

    @SuppressWarnings("unchecked")
    private static long[] queryMinMax(Database dialect, DatabaseConnectorInstance db,
                                      String schema, String tableName, String pkName) {
        try {
            String sql = buildMinMaxSql(dialect, schema, tableName, pkName);
            Map<String, Object> row = db.execute(template -> template.queryForMap(sql));
            if (row == null || row.isEmpty()) {
                return null;
            }
            Object minObj = firstColumn(row, "MN");
            Object maxObj = firstColumn(row, "MX");
            if (minObj == null || maxObj == null) {
                return null;
            }
            long min = ((Number) minObj).longValue();
            long max = ((Number) maxObj).longValue();
            if (max < min) {
                return null;
            }
            return new long[]{min, max};
        } catch (Exception e) {
            LOGGER.warn("查询主键边界失败, table={}, err={}", tableName, e.getMessage());
            return null;
        }
    }

    private static Object firstColumn(Map<String, Object> row, String alias) {
        if (row == null || StringUtil.isBlank(alias)) {
            return null;
        }
        Object direct = row.get(alias);
        if (direct != null) {
            return direct;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry != null && entry.getKey() != null && alias.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String buildMinMaxSql(Database dialect, String schema, String tableName, String pkName) {
        String qTable = dialect.buildWithQuotation(tableName);
        String qPk = dialect.buildWithQuotation(pkName);
        String from = qTable;
        if (StringUtil.isNotBlank(schema)) {
            from = dialect.buildWithQuotation(schema) + "." + qTable;
        }
        return "SELECT MIN(" + qPk + ") AS MN, MAX(" + qPk + ") AS MX FROM " + from;
    }

    /**
     * 把 RANGE/HASH 条件插入到 SQL 的 WHERE 段（须在 ORDER BY / LIMIT / OFFSET 之前）。
     * <p>SQL Server {@code ROW_NUMBER} 分页须写入最内层 {@code FROM (base) S}，
     * 不可插在 {@code OVER(ORDER BY)} 前。
     *
     * @param dialect 方言
     * @param querySql 原查询
     * @param shard    切片
     * @param args     参数列表（就地追加切片参数）
     * @param pkField  主键字段（HASH 需要类型）；可为 null
     * @return 新 SQL；无法下推返回原 SQL
     */
    public static String appendShardCondition(Database dialect, String querySql, ShardSpec shard,
                                              List<Object> args, Field pkField) {
        if (dialect == null || StringUtil.isBlank(querySql) || shard == null || shard.isWhole()) {
            return querySql;
        }
        String condition = buildShardCondition(dialect, shard, args, pkField);
        if (StringUtil.isBlank(condition)) {
            return querySql;
        }
        if (isSqlServerPageSql(querySql)) {
            return appendIntoSqlServerInner(querySql, condition);
        }
        String keyword = containsWhereClause(querySql) ? " AND " : " WHERE ";
        return insertBeforeTrailingClauses(querySql, keyword + condition);
    }

    /**
     * 是否已有 WHERE 子句（忽略 ORDER BY / LIMIT 之后的文本）。
     */
    static boolean containsWhereClause(String querySql) {
        if (StringUtil.isBlank(querySql)) {
            return false;
        }
        String head = querySql.substring(0, indexOfTrailingClause(querySql));
        return head.toUpperCase().contains(" WHERE ");
    }

    /**
     * 在 ORDER BY / LIMIT / OFFSET / FETCH 之前插入片段。
     */
    static String insertBeforeTrailingClauses(String querySql, String clause) {
        if (StringUtil.isBlank(querySql) || StringUtil.isBlank(clause)) {
            return querySql;
        }
        int at = indexOfTrailingClause(querySql);
        if (at >= querySql.length()) {
            return querySql + clause;
        }
        return querySql.substring(0, at) + clause + querySql.substring(at);
    }

    /**
     * SQL Server 分页：条件写入最内层 base query。
     */
    static String appendIntoSqlServerInner(String querySql, String condition) {
        if (StringUtil.isBlank(querySql) || StringUtil.isBlank(condition)) {
            return querySql;
        }
        String upper = querySql.toUpperCase();
        int end = upper.lastIndexOf(") S)");
        int from = end > 0 ? upper.lastIndexOf("FROM (", end) : -1;
        if (end < 0 || from < 0) {
            String keyword = containsWhereClause(querySql) ? " AND " : " WHERE ";
            return insertBeforeTrailingClauses(querySql, keyword + condition);
        }
        int innerStart = from + "FROM (".length();
        String inner = querySql.substring(innerStart, end);
        String keyword = containsWhereClause(inner) ? " AND " : " WHERE ";
        String newInner = insertBeforeTrailingClauses(inner, keyword + condition);
        return querySql.substring(0, innerStart) + newInner + querySql.substring(end);
    }

    static boolean isSqlServerPageSql(String querySql) {
        if (StringUtil.isBlank(querySql)) {
            return false;
        }
        String upper = querySql.toUpperCase();
        return upper.contains("SQLSERVER_ROW_ID") && upper.contains(") S)");
    }

    private static int indexOfTrailingClause(String querySql) {
        String upper = querySql.toUpperCase();
        int orderBy = indexOfToken(upper, " ORDER BY ");
        int limit = indexOfToken(upper, " LIMIT ");
        int offset = indexOfToken(upper, " OFFSET ");
        int fetch = indexOfToken(upper, " FETCH ");
        int at = querySql.length();
        if (orderBy >= 0) {
            at = Math.min(at, orderBy);
        }
        if (limit >= 0) {
            at = Math.min(at, limit);
        }
        if (offset >= 0) {
            at = Math.min(at, offset);
        }
        if (fetch >= 0) {
            at = Math.min(at, fetch);
        }
        return at;
    }

    private static int indexOfToken(String upperSql, String token) {
        int idx = upperSql.indexOf(token);
        return idx < 0 ? -1 : idx;
    }

    private static String buildShardCondition(Database dialect, ShardSpec shard, List<Object> args, Field pkField) {
        switch (shard.getCapability()) {
            case RANGE:
                return buildRangeCondition(dialect, shard, args, pkField);
            case HASH_MOD:
                return dialect.buildHashModCondition(shard, args, pkField);
            default:
                return null;
        }
    }

    private static String buildRangeCondition(Database dialect, ShardSpec shard, List<Object> args, Field pkField) {
        String pk = StringUtil.getIfBlank(shard.payload(ShardSpec.KEY_PK), pkField == null ? null : pkField.getName());
        String from = shard.payload(ShardSpec.KEY_FROM);
        String to = shard.payload(ShardSpec.KEY_TO);
        if (StringUtil.isBlank(pk) || StringUtil.isBlank(from) || StringUtil.isBlank(to)) {
            return null;
        }
        String qPk = dialect.buildWithQuotation(pk);
        if (NumberUtil.isCreatable(from) && NumberUtil.isCreatable(to)) {
            args.add(NumberUtil.toLong(from));
            args.add(NumberUtil.toLong(to));
        } else {
            args.add(from);
            args.add(to);
        }
        return qPk + " >= ? AND " + qPk + " <= ?";
    }
}
