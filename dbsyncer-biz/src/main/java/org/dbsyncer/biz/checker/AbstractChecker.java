package org.dbsyncer.biz.checker;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public abstract class AbstractChecker implements Checker {

    @Override
    public void modify(Connector connector, Map<String, String> params) {
        throw new BizException("Un supported.");
    }

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        throw new BizException("Un supported.");
    }

}