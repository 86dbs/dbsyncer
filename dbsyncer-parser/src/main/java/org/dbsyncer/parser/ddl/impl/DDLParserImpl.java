package org.dbsyncer.parser.ddl.impl;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * alter情况
 * <ol>
 *     <li>只是修改字段的属性值</li>
 *     <li>修改字段的名称</li>
 * </ol>
 *
 * @author life
 * @version 1.0.0
 * @date 2023/9/19 22:38
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public DDLConfig parseDDlConfig(String sql, String targetConnectorType, String targetTableName,
            List<FieldMapping> fieldMappingList) {
        Connector connector = connectorFactory.getConnector(targetConnectorType);
        // 暂支持关系型数据库解析
        if (!(connector instanceof Database)) {
            throw new ParserException("暂支持关系型数据库解析");
        }

        // 替换为目标库执行SQL
        DDLConfig ddlConfig = new DDLConfig();
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                Database database = (Database) connector;
                String quotation = database.buildSqlWithQuotation();
                String tableName = new StringBuilder(quotation).append(targetTableName).append(quotation).toString();
                // 替换成目标表名
                alter.getTable().setName(tableName);
                logger.info("转换目标源sql:{}", alter);
                for (AlterExpression expression: alter.getAlterExpressions()) {
                    AlterOperation alterOperation = expression.getOperation();
                    if (alterOperation == AlterOperation.MODIFY){//修改属性
                        //先查找到当前的表和目标的表对应的字段
                        for (AlterExpression.ColumnDataType columnDataType:expression.getColDataTypeList()) {
                            String columName = columnDataType.getColumnName();
                            columName = columName.replaceAll("`","");
                            for (FieldMapping fieldMapping :fieldMappingList) {
                                if (fieldMapping.getSource().getName().equals(columName)){
                                    //TODO life 找到目标的表名，先是alter进行属性替换，然后config记录新的
                                    columnDataType.setColumnName(fieldMapping.getTarget().getName());//alter语法树进行替换
                                    //因为只是修改属性，所以表名称没有变化
                                    ddlConfig.setSourceColumnName(fieldMapping.getSource().getName());
                                    ddlConfig.setChangedColumnName(fieldMapping.getSource().getName());
                                    break;
                                }
                            }
                        }
                    }else if (AlterOperation.ADD == alterOperation){//新增字段

                    }
                }
                ddlConfig.setSql(alter.toString());
            }
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
        return ddlConfig;
    }

}