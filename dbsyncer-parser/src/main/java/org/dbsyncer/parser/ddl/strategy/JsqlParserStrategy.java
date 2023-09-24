package org.dbsyncer.parser.ddl.strategy;

import java.util.List;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * @author life
 */
public interface JsqlParserStrategy extends Strategy{

    public void parser();
}
