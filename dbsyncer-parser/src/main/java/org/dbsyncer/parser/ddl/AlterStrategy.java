package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.sdk.config.DDLConfig;

import java.util.List;

/**
 * Alter策略
 *
 * @version 1.0.0
 * @Author life
 * @Date 2023-09-24 14:24
 */
public interface AlterStrategy {

    /**
     * 解析DDLConfig
     *
     * @param expression
     * @param ddlConfig
     * @param fieldMappingList
     */
    void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> fieldMappingList);
}
