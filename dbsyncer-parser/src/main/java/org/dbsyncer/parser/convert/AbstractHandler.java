package org.dbsyncer.parser.convert;

import org.dbsyncer.parser.ParserException;

import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.InputStream;

public abstract class AbstractHandler implements Handler {

    /**
     * 交给实现handler
     *
     * @param args  参数
     * @param value 值
     * @return
     */
    protected abstract Object convert(String args, Object value) throws Exception;

    @Override
    public Object handle(String args, Object value) {
        if (null != value) {
            try {
                return convert(args, value);
            } catch (Exception e) {
                throw new ParserException(e.getMessage());
            }
        }
        return null;
    }

    protected String getString(InputStream in, int length) {
        BufferedInputStream is = null;
        try {
            is = new BufferedInputStream(in);
            byte[] bytes = new byte[length];
            int len = bytes.length;
            int offset = 0;
            int read;
            while (offset < len && (read = is.read(bytes, offset, len - offset)) >= 0) {
                offset += read;
            }
            return new String(bytes);
        } catch (Exception e) {
            throw new ParserException(e.getMessage());
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
}
