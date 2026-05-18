/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 自定义 JDBC 参数绑定（如 Oracle CLOB 需通过连接创建后 setClob）。
 */
public interface BindParameter {

    void setValue(PreparedStatement ps, int paramIndex, Connection connection) throws SQLException;
}
