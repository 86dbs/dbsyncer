package org.dbsyncer.connector.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.sqlbuilder.*;
import org.dbsyncer.connector.database.sqlbuilder.SqlBuilder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:13
 */
public enum SqlBuilderEnum {

    /**
     * 插入SQL生成器
     */
    INSERT(ConnectorConstant.OPERTION_INSERT, new SqlBuilderInsert()),
    /**
     * 修改SQL生成器
     */
    UPDATE(ConnectorConstant.OPERTION_UPDATE, new SqlBuilderUpdate()),
    /**
     * 删除SQL生成器
     */
    DELETE(ConnectorConstant.OPERTION_DELETE, new SqlBuilderDelete()),
    /**
     * 查询SQL生成器
     */
    QUERY(ConnectorConstant.OPERTION_QUERY, new SqlBuilderQuery());

    /**
     * SQL构造器名称
     */
    private String name;

    /**
     * SQL构造器
     */
    private SqlBuilder sqlBuilder;

    SqlBuilderEnum(String name, SqlBuilder sqlBuilder) {
        this.name = name;
        this.sqlBuilder = sqlBuilder;
    }

    public static SqlBuilder getSqlBuilder(String name) throws ConnectorException {
        for (SqlBuilderEnum e : SqlBuilderEnum.values()) {
            if (StringUtil.equals(name, e.getName())) {
                return e.getSqlBuilder();
            }
        }
        throw new ConnectorException(String.format("SqlBuilder name \"%s\" does not exist.", name));
    }

    public String getName() {
        return name;
    }

    public SqlBuilder getSqlBuilder() {
        return sqlBuilder;
    }

}