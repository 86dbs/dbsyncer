package org.dbsyncer.connector.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class AbstractSetter implements Setter {

    /**
     * 实现字段类型参数设置
     *
     * @param ps
     * @param i
     * @param val
     */
    protected abstract void set(PreparedStatement ps, int i, Object val) throws SQLException;

    @Override
    public void set(PreparedStatement ps, int i, int type, Object val) {
        try {
            if (null == val) {
                ps.setNull(i, type);
            } else {
                set(ps, i, val);
            }
        } catch (Exception e) {
            try {
                ps.setNull(i, type);
            } catch (SQLException e1) {
            }
        }
    }
    
}