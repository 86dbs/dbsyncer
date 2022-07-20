package org.dbsyncer.connector.database.setter;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class VarcharSetter extends AbstractSetter<String> {

    @Override
    protected void set(PreparedStatement ps, int i, String val) throws SQLException {
        ps.setString(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if (val instanceof byte[]) {
            ps.setString(i, new String((byte[]) val));
            return;
        } else if (val instanceof LocalDateTime) {
            ps.setString(i, ((LocalDateTime) val).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            return;
        } else if (val instanceof LocalDate) {
            ps.setString(i, ((LocalDate) val).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            return;
        } else if (val instanceof Date) {
            ps.setString(i, DateFormatUtil.dateToChineseStandardTimeString((Date) val));
            return;
        } else {
            ps.setString(i, val.toString());
        }
//        throw new ConnectorException(String.format("VarcharSetter can not find type [%s], val [%s]", type, val));
    }
}