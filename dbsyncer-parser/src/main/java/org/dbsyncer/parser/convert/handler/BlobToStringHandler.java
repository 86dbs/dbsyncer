package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.AbstractHandler;

import java.sql.SQLException;

/**
 * Blob转String
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/1/20 23:04
 */
public class BlobToStringHandler extends AbstractHandler {

    @Override
    public Object convert(String args, Object value) {
        if (value instanceof oracle.sql.BLOB) {
            oracle.sql.BLOB blob = (oracle.sql.BLOB) value;
            try {
                value = getString(blob.getBinaryStream(), (int) blob.length());
            } catch (SQLException e) {
                throw new ParserException(e.getMessage());
            }
        }
        return value;
    }

}