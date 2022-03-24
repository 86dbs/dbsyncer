package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.SQLException;

/**
 * Clob转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/1/20 23:04
 */
public class ClobToStringHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof oracle.sql.CLOB) {
            oracle.sql.CLOB clob = (oracle.sql.CLOB) value;
            try {
                value = getString(clob.getAsciiStream(), (int) clob.length());
            } catch (SQLException e) {
                throw new ParserException(e.getMessage());
            }
        }
        return value;
    }

}