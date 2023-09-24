package org.dbsyncer.parser.ddl.alter;

import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * @author life
 */
public interface ExpressionStrategy {
    void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> fieldMappingList);
}
