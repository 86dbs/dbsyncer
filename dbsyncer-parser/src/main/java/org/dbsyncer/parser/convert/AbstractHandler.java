package org.dbsyncer.parser.convert;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.parser.ParserException;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Map;

public abstract class AbstractHandler implements Handler {

    /**
     * 交给实现handler
     *
     * @param args  参数
     * @param value 值
     * @param row   数据行（可为 null，某些 Handler 不需要）
     * @return
     */
    protected abstract Object convert(String args, Object value, Map<String, Object> row) throws Exception;

    @Override
    public Object handle(String args, Object value, Map<String, Object> row) {
        if (null != value) {
            try {
                return convert(args, value, row);
            } catch (Exception e) {
                throw new ParserException(e.getMessage());
            }
        }
        return null;
    }

    protected String getString(InputStream in, int length){
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