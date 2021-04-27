package org.dbsyncer.connector.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class AbstractSetter<T> implements Setter {

    private final Logger   logger = LoggerFactory.getLogger(getClass());
    private final Class<T> parameterClazz;

    public AbstractSetter() {
        parameterClazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * 实现字段类型参数设置
     *
     * @param ps
     * @param i
     * @param val
     */
    protected abstract void set(PreparedStatement ps, int i, T val) throws SQLException;

    /**
     * 参数类型不匹配
     *
     * @param val
     * @return
     */
    protected void setIfValueTypeNotMatch(PreparedStatement ps, int i, int type, Object val) throws SQLException {
        ps.setNull(i, type);
    }

    @Override
    public void set(PreparedStatement ps, int i, int type, Object val) {
        try {
            if (null == val) {
                ps.setNull(i, type);
                return;
            }

            if (val.getClass().equals(parameterClazz)) {
                set(ps, i, (T) (val));
                return;
            }

            setIfValueTypeNotMatch(ps, i, type, val);
        } catch (Exception e) {
            logger.error("Set preparedStatement error: {}", e.getMessage());
            try {
                ps.setNull(i, type);
            } catch (SQLException e1) {
            }
        }
    }

}