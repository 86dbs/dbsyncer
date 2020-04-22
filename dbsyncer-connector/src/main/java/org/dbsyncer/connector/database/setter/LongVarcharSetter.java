package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongVarcharSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        // 当数据库为mysql,字段类型为text,如果值里面包含非数字类型转换会失败,为兼容采用Types.VARCHAR方式替换
        ps.setString(i, String.valueOf(val));
    }

}