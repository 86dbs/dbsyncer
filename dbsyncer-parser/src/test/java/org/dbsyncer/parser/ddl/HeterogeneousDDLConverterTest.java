package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.impl.HeterogeneousDDLConverterImpl;
import org.dbsyncer.parser.ddl.impl.MySQLToSQLServerConverter;
import org.dbsyncer.parser.ddl.impl.SQLServerToMySQLConverter;
import org.dbsyncer.sdk.config.DDLConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * 异构数据库DDL转换器测试
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 21:00
 */
public class HeterogeneousDDLConverterTest {

    private static final Logger logger = LoggerFactory.getLogger(HeterogeneousDDLConverterTest.class);

    private HeterogeneousDDLConverterImpl converter;
    private MySQLToSQLServerConverter mySQLToSQLServerConverter;
    private SQLServerToMySQLConverter sqlServerToMySQLConverter;

    @Before
    public void setUp() {
        converter = new HeterogeneousDDLConverterImpl();
        mySQLToSQLServerConverter = new MySQLToSQLServerConverter();
        sqlServerToMySQLConverter = new SQLServerToMySQLConverter();
    }

    @Test
    public void testMySQLToSQLServerAddColumn() throws JSQLParserException {
        logger.info("开始测试MySQL到SQL Server的ADD COLUMN转换");
        
        String mysqlDDL = "ALTER TABLE test_table ADD COLUMN salary DECIMAL(10,2)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(mysqlDDL);
        
        DDLConfig ddlConfig = new DDLConfig();
        String result = mySQLToSQLServerConverter.convert("MySQL", "SqlServer", alter, ddlConfig);
        
        logger.info("源DDL: {}", mysqlDDL);
        logger.info("目标DDL: {}", result);
        
        // 验证转换结果
        assertTrue("转换结果不应为空", result != null && !result.isEmpty());
        assertTrue("应包含ALTER TABLE关键字", result.contains("ALTER TABLE"));
        assertTrue("应包含ADD关键字", result.contains("ADD"));
        assertTrue("应不包含COLUMN关键字", !result.contains("COLUMN")); // SQL Server不需要COLUMN关键字
        assertTrue("应包含salary字段", result.contains("salary"));
        assertTrue("应包含DECIMAL类型", result.contains("DECIMAL(10,2)"));
        
        logger.info("MySQL到SQL Server的ADD COLUMN转换测试通过");
    }

    @Test
    public void testMySQLToSQLServerModifyColumn() throws JSQLParserException {
        logger.info("开始测试MySQL到SQL Server的MODIFY COLUMN转换");
        
        String mysqlDDL = "ALTER TABLE test_table MODIFY COLUMN name VARCHAR(100)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(mysqlDDL);
        
        DDLConfig ddlConfig = new DDLConfig();
        String result = mySQLToSQLServerConverter.convert("MySQL", "SqlServer", alter, ddlConfig);
        
        logger.info("源DDL: {}", mysqlDDL);
        logger.info("目标DDL: {}", result);
        
        // 验证转换结果
        assertTrue("转换结果不应为空", result != null && !result.isEmpty());
        assertTrue("应包含ALTER TABLE关键字", result.contains("ALTER TABLE"));
        assertTrue("应包含ALTER COLUMN关键字", result.contains("ALTER COLUMN"));
        assertTrue("应包含name字段", result.contains("name"));
        assertTrue("应包含VARCHAR类型", result.contains("VARCHAR(100)"));
        
        logger.info("MySQL到SQL Server的MODIFY COLUMN转换测试通过");
    }

    @Test
    public void testSQLServerToMySQLAddColumn() throws JSQLParserException {
        logger.info("开始测试SQL Server到MySQL的ADD COLUMN转换");
        
        String sqlserverDDL = "ALTER TABLE test_table ADD salary DECIMAL(10,2)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(sqlserverDDL);
        
        DDLConfig ddlConfig = new DDLConfig();
        String result = sqlServerToMySQLConverter.convert("SqlServer", "MySQL", alter, ddlConfig);
        
        logger.info("源DDL: {}", sqlserverDDL);
        logger.info("目标DDL: {}", result);
        
        // 验证转换结果
        assertTrue("转换结果不应为空", result != null && !result.isEmpty());
        assertTrue("应包含ALTER TABLE关键字", result.contains("ALTER TABLE"));
        assertTrue("应包含ADD COLUMN关键字", result.contains("ADD COLUMN"));
        assertTrue("应包含salary字段", result.contains("salary"));
        assertTrue("应包含DECIMAL类型", result.contains("DECIMAL(10,2)"));
        
        logger.info("SQL Server到MySQL的ADD COLUMN转换测试通过");
    }

    @Test
    public void testSQLServerToMySQLAlterColumn() throws JSQLParserException {
        logger.info("开始测试SQL Server到MySQL的ALTER COLUMN转换");
        
        String sqlserverDDL = "ALTER TABLE test_table ALTER COLUMN name NVARCHAR(100)";
        Alter alter = (Alter) CCJSqlParserUtil.parse(sqlserverDDL);
        
        DDLConfig ddlConfig = new DDLConfig();
        String result = sqlServerToMySQLConverter.convert("SqlServer", "MySQL", alter, ddlConfig);
        
        logger.info("源DDL: {}", sqlserverDDL);
        logger.info("目标DDL: {}", result);
        
        // 验证转换结果
        assertTrue("转换结果不应为空", result != null && !result.isEmpty());
        assertTrue("应包含ALTER TABLE关键字", result.contains("ALTER TABLE"));
        assertTrue("应包含MODIFY COLUMN关键字", result.contains("MODIFY COLUMN")); // SQL Server的ALTER COLUMN转换为MySQL的MODIFY COLUMN
        assertTrue("应包含name字段", result.contains("name"));
        assertTrue("应包含VARCHAR类型", result.contains("VARCHAR(100)")); // NVARCHAR转换为VARCHAR
        
        logger.info("SQL Server到MySQL的ALTER COLUMN转换测试通过");
    }

    @Test
    public void testConverterSupports() {
        logger.info("开始测试转换器支持检查");
        
        assertTrue("应支持MySQL到SQL Server转换", 
            mySQLToSQLServerConverter.supports("MySQL", "SqlServer"));
        assertTrue("应支持SQL Server到MySQL转换", 
            sqlServerToMySQLConverter.supports("SqlServer", "MySQL"));
        assertFalse("不应支持MySQL到MySQL转换", 
            mySQLToSQLServerConverter.supports("MySQL", "MySQL"));
        assertFalse("不应支持SQL Server到SQL Server转换", 
            mySQLToSQLServerConverter.supports("SqlServer", "SqlServer"));
            
        logger.info("转换器支持检查测试通过");
    }
}