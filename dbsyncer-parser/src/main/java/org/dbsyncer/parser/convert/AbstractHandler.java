package org.dbsyncer.parser.convert;

import org.dbsyncer.parser.ParserException;

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
}