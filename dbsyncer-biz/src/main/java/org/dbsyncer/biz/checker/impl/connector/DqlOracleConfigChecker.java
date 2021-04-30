package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.parser.model.Connector;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class DqlOracleConfigChecker extends AbstractDataBaseConfigChecker {

    @Override
    public void modify(Connector connector, Map<String, String> params) {
        super.modify(connector, params);
        super.modifyDql(connector, params);
    }
}