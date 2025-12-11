package org.dbsyncer.web.integration;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.connector.mysql.converter.IRToMySQLConverter;
import org.dbsyncer.connector.mysql.converter.MySQLToIRConverter;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.connector.sqlserver.converter.IRToSQLServerConverter;
import org.dbsyncer.connector.sqlserver.converter.SQLServerToIRConverter;
import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.sdk.connector.database.sql.impl.MySQLTemplate;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * 基于中间表示的异构数据库DDL转换器测试
 * 
 * 这是一个单元测试，测试DDL转换器本身的功能，不需要数据库连接
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-27 11:00
 */
public class IRBasedHeterogeneousDDLConverterTest {

    private static final Logger logger = LoggerFactory.getLogger(IRBasedHeterogeneousDDLConverterTest.class);

    private MySQLToIRConverter mySQLToIRConverter;
    private SQLServerToIRConverter sqlServerToIRConverter;
    private IRToMySQLConverter irToMySQLConverter;
    private IRToSQLServerConverter irToSQLServerConverter;

    @Before
    public void setUp() {
        mySQLToIRConverter = new MySQLToIRConverter();
        sqlServerToIRConverter = new SQLServerToIRConverter();
        // 创建带有SqlTemplate的转换器实例
        irToMySQLConverter = new IRToMySQLConverter(new MySQLTemplate(new MySQLSchemaResolver()));
        irToSQLServerConverter = new IRToSQLServerConverter(new SqlServerTemplate(new SqlServerSchemaResolver()));
    }

    @Test
    public void testMySQLToIR() throws JSQLParserException {
        logger.info("开始测试MySQL到中间表示的转换");

        String mysqlDDL = "ALTER TABLE test_table ADD COLUMN salary DECIMAL(10,2)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(mysqlDDL);

        DDLIntermediateRepresentation ir = mySQLToIRConverter.convert(alter);

        logger.info("源DDL: {}", mysqlDDL);
        logger.info("中间表示表名: {}", ir.getTableName());
        logger.info("中间表示操作类型: {}", ir.getOperationType());
        logger.info("中间表示列数: {}", ir.getColumns().size());

        // 验证转换结果
        assertNotNull("中间表示不应为空", ir);
        assertEquals("表名应正确转换", "test_table", ir.getTableName());
        assertEquals("操作类型应为ADD", "ADD", ir.getOperationType().name());
        assertEquals("应包含1个列", 1, ir.getColumns().size());
        assertEquals("列名应为salary", "salary", ir.getColumns().get(0).getName());
        assertEquals("数据类型应为DECIMAL", "DECIMAL", ir.getColumns().get(0).getTypeName());
        assertEquals("列大小应为10", 10, ir.getColumns().get(0).getColumnSize());
        assertEquals("比例应为2", 2, ir.getColumns().get(0).getRatio());

        logger.info("MySQL到中间表示的转换测试通过");
    }

    @Test
    public void testIRToSQLServer() throws JSQLParserException {
        logger.info("开始测试中间表示到SQL Server的转换");

        String mysqlDDL = "ALTER TABLE test_table ADD COLUMN name VARCHAR(50)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(mysqlDDL);

        // MySQL → IR
        DDLIntermediateRepresentation ir = mySQLToIRConverter.convert(alter);

        // IR → SQL Server
        String sqlServerDDL = irToSQLServerConverter.convert(ir);

        logger.info("源MySQL DDL: {}", mysqlDDL);
        logger.info("目标SQL Server DDL: {}", sqlServerDDL);

        // 验证转换结果
        assertNotNull("目标DDL不应为空", sqlServerDDL);
        assertTrue("应包含ALTER TABLE关键字", sqlServerDDL.contains("ALTER TABLE"));
        assertTrue("应包含ADD关键字", sqlServerDDL.contains("ADD"));
        assertTrue("应不包含COLUMN关键字", !sqlServerDDL.contains("COLUMN")); // SQL Server不需要COLUMN关键字
        assertTrue("应包含name字段", sqlServerDDL.contains("name"));
        assertTrue("应包含NVARCHAR类型", sqlServerDDL.contains("NVARCHAR(50)"));

        logger.info("中间表示到SQL Server的转换测试通过");
    }

    @Test
    public void testMySQLToSQLServerFullConversion() throws JSQLParserException {
        logger.info("开始测试MySQL到SQL Server的完整转换");

        String mysqlDDL = "ALTER TABLE users ADD COLUMN created_at DATETIME";
        Alter alter = (Alter) CCJSqlParserUtil.parse(mysqlDDL);

        // 完整转换流程：MySQL → IR → SQL Server
        DDLIntermediateRepresentation ir = mySQLToIRConverter.convert(alter);
        String sqlServerDDL = irToSQLServerConverter.convert(ir);

        logger.info("源MySQL DDL: {}", mysqlDDL);
        logger.info("目标SQL Server DDL: {}", sqlServerDDL);

        // 验证转换结果
        assertNotNull("目标DDL不应为空", sqlServerDDL);
        assertTrue("应包含ALTER TABLE关键字", sqlServerDDL.contains("ALTER TABLE"));
        assertTrue("应包含ADD关键字", sqlServerDDL.contains("ADD"));
        assertTrue("应不包含COLUMN关键字", !sqlServerDDL.contains("COLUMN")); // SQL Server不需要COLUMN关键字
        assertTrue("应包含created_at字段", sqlServerDDL.contains("created_at"));
        assertTrue("DATETIME应转换为DATETIME2", sqlServerDDL.contains("DATETIME2"));

        logger.info("MySQL到SQL Server的完整转换测试通过");
    }

    @Test
    public void testSQLServerToIR() throws JSQLParserException {
        logger.info("开始测试SQL Server到中间表示的转换");

        String sqlserverDDL = "ALTER TABLE test_table ADD salary DECIMAL(10,2)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(sqlserverDDL);

        DDLIntermediateRepresentation ir = sqlServerToIRConverter.convert(alter);

        logger.info("源DDL: {}", sqlserverDDL);
        logger.info("中间表示表名: {}", ir.getTableName());
        logger.info("中间表示操作类型: {}", ir.getOperationType());
        logger.info("中间表示列数: {}", ir.getColumns().size());

        // 验证转换结果
        assertNotNull("中间表示不应为空", ir);
        assertEquals("表名应正确转换", "test_table", ir.getTableName());
        assertEquals("操作类型应为ADD", "ADD", ir.getOperationType().name());
        assertEquals("应包含1个列", 1, ir.getColumns().size());
        assertEquals("列名应为salary", "salary", ir.getColumns().get(0).getName());
        assertEquals("数据类型应为DECIMAL", "DECIMAL", ir.getColumns().get(0).getTypeName());
        assertEquals("列大小应为10", 10, ir.getColumns().get(0).getColumnSize());
        assertEquals("比例应为2", 2, ir.getColumns().get(0).getRatio());

        logger.info("SQL Server到中间表示的转换测试通过");
    }

    @Test
    public void testIRToMySQL() throws JSQLParserException {
        logger.info("开始测试中间表示到MySQL的转换");

        String sqlserverDDL = "ALTER TABLE test_table ADD salary DECIMAL(10,2)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(sqlserverDDL);

        // SQL Server → IR
        DDLIntermediateRepresentation ir = sqlServerToIRConverter.convert(alter);

        // IR → MySQL
        String mysqlDDL = irToMySQLConverter.convert(ir);

        logger.info("源SQL Server DDL: {}", sqlserverDDL);
        logger.info("目标MySQL DDL: {}", mysqlDDL);

        // 验证转换结果
        assertNotNull("目标DDL不应为空", mysqlDDL);
        assertTrue("应包含ALTER TABLE关键字", mysqlDDL.contains("ALTER TABLE"));
        assertTrue("应包含ADD COLUMN关键字", mysqlDDL.contains("ADD COLUMN"));
        assertTrue("应包含salary字段", mysqlDDL.contains("salary"));
        assertTrue("应包含DECIMAL类型", mysqlDDL.contains("DECIMAL(10,2)"));

        logger.info("中间表示到MySQL的转换测试通过");
    }
}








