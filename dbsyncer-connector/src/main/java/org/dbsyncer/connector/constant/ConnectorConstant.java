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
     * 查询最近记录点
     * <p>例如：SELECT MAX(MY_TEST.LAST_TIME) FROM MY_TEST</p>
     */
    public static final String OPERTION_QUERY_MAX = "QUERY_MAX";

    /**
     * 查询表达式and
     */
    public static final String OPERTION_QUERY_AND = "and";

    /**
     * 查询表达式or
     */
    public static final String OPERTION_QUERY_OR = "or";

}
