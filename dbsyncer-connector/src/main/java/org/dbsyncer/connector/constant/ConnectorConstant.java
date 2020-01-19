package org.dbsyncer.connector.constant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public class ConnectorConstant {

    /**
     * 新增
     */
    public static final String OPERTION_INSERT = "INSERT";

    /**
     * 更新
     */
    public static final String OPERTION_UPDATE = "UPDATE";

    /**
     * 删除
     */
    public static final String OPERTION_DELETE = "DELETE";

    /**
     * 查询
     */
    public static final String OPERTION_QUERY = "QUERY";

    /**
     * 查询表达式and
     */
    public static final String OPERTION_QUERY_AND = "and";

    /**
     * 查询表达式or
     */
    public static final String OPERTION_QUERY_OR = "or";

    /**
     * 日志_增量查询
     * <p>例如：WHERE USER.ID = ?</p>
     */
    public static final String OPERTION_QUERY_LOG = "QUERY_LOG";

    /**
     * 定时器_增量查询
     * <p>例如：SELECT ASD_TEST.* FROM ASD_TEST</p>
     */
    public static final String OPERTION_QUERY_QUARTZ = "QUERY_QUARTZ";

    /**
     * 定时器_增量查询条件:增量字段 < 增量时间 <= 增量字段
     * <p>T1.LASTDATE > '2017-11-10 11:07:41' AND T1.LASTDATE <= '2017-11-10 11:30:01' ORDER BY LASTDATE</p>
     */
    public static final String OPERTION_QUERY_QUARTZ_RANGE = "QUERY_QUARTZ_RANGE";

    /**
     * 定时器_增量查询条件:增量字段<= 增量时间
     * <p>T1.LASTDATE <= '2017-11-10 11:07:41' ORDER BY T1.LASTDATE</p>
     */
    public static final String OPERTION_QUERY_QUARTZ_ALL = "QUERY_QUARTZ_ALL";

    /**
     * 定时器_增量查询最后记录点
     * <p>例如：SELECT MAX(T1.LASTDATE) FROM ASD_TEST T1</p>
     */
    public static final String OPERTION_QUERY_QUARTZ_MAX = "QUERY_QUARTZ_MAX";

}
