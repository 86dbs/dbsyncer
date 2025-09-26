# DBSyncer SQL模板接口设计文档

## 1. 背景与目标

### 1.1 当前问题分析

通过对DBSyncer项目的深入分析，发现以下SQL使用问题：

1. **SQL分散且重复**：各个数据库连接器中存在大量相似的SQL构建逻辑
2. **维护困难**：SQL逻辑分散在多个连接器中，修改需要同步更新多个地方
3. **代码重复**：不同连接器实现相同的SQL构建模式，如游标查询等
4. **扩展性差**：新增数据库支持需要重复实现相似的SQL逻辑

### 1.2 设计目标

- **统一SQL模板**：提供一套通用的SQL模板接口，适用于所有数据库连接器
- **减少重复代码**：消除各连接器中的重复SQL构建逻辑
- **提高可维护性**：集中管理SQL模板，便于统一修改和优化
- **支持扩展**：允许特定数据库连接器覆盖默认模板，满足特殊需求
- **向后兼容**：不影响现有连接器的功能和使用方式

## 2. 现状分析

### 2.1 SQL使用分布情况

通过代码分析发现，项目中SQL主要分布在以下位置：

#### 2.1.1 核心SQL构建器
- `SqlBuilderInsert` - 插入SQL生成
- `SqlBuilderUpdate` - 更新SQL生成  
- `SqlBuilderDelete` - 删除SQL生成
- `SqlBuilderQuery` - 查询SQL生成
- `SqlBuilderQueryCount` - 计数SQL生成
- `SqlBuilderQueryExist` - 存在性检查SQL生成

#### 2.1.2 数据库特定SQL常量
```java
// DatabaseConstant.java 中定义的各数据库SQL常量
MYSQL_PAGE_SQL = " LIMIT ?,?"
ORACLE_PAGE_SQL_START = "SELECT * FROM (SELECT A.*, ROWNUM RN FROM ("
ORACLE_PAGE_SQL_END = ")A WHERE ROWNUM <= ?) WHERE RN > ?"
SQLSERVER_PAGE_SQL = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY %s) AS SQLSERVER_ROW_ID, * FROM (%s) S) A WHERE A.SQLSERVER_ROW_ID BETWEEN ? AND ?"
POSTGRESQL_PAGE_SQL = " limit ? OFFSET ?"
SQLITE_PAGE_SQL = " limit ? OFFSET ?"
```

#### 2.1.3 连接器中的SQL构建逻辑
各连接器在`buildSourceCommands`方法中重复实现相似的SQL构建逻辑：

**MySQL连接器示例**：
```java
String streamingSql = String.format("SELECT %s FROM %s%s%s",
    fieldListSql, schema, quotedTableName, filterClause);
```

**SQL Server连接器示例**：
```java
String cursorSql = String.format("SELECT %s FROM %s%s%s ORDER BY %s OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
    fieldListSql, schema, quotedTableName, whereCondition, commandConfig.getCachedPrimaryKeys());
```

**Oracle连接器示例**：
```java
String cursorSql = String.format("SELECT * FROM (SELECT ROWNUM rn, t.* FROM (SELECT %s FROM %s%s%s ORDER BY %s) t WHERE ROWNUM <= ?) WHERE rn > ?",
    fieldListSql, schema, quotedTableName, whereCondition, commandConfig.getCachedPrimaryKeys());
```

### 2.2 通用模式识别

通过分析发现以下通用SQL模式：

1. **基础查询SQL**：`SELECT fields FROM schema.table WHERE conditions`
2. **流式查询SQL**：基础查询 + 数据库特定的流式处理
3. **游标查询SQL**：基础查询 + 游标条件 + 数据库特定的游标语法
4. **计数查询SQL**：`SELECT COUNT(1) FROM schema.table WHERE conditions`
5. **插入SQL**：`INSERT INTO schema.table (fields) VALUES (?)`
6. **更新SQL**：`UPDATE schema.table SET fields=? WHERE primary_keys=?`
7. **删除SQL**：`DELETE FROM schema.table WHERE primary_keys=?`

## 3. 设计方案

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL模板接口层                              │
├─────────────────────────────────────────────────────────────┤
│  SqlTemplate (接口)                                         │
│  ├── DefaultSqlTemplate (默认实现)                          │
│  ├── MySQLTemplate (MySQL特定实现)                          │
│  ├── OracleTemplate (Oracle特定实现)                         │
│  ├── SqlServerTemplate (SQL Server特定实现)                  │
│  └── PostgreSQLTemplate (PostgreSQL特定实现)                 │
├─────────────────────────────────────────────────────────────┤
│                   连接器层                                   │
├─────────────────────────────────────────────────────────────┤
│  AbstractDatabaseConnector (抽象连接器)                       │
│  ├── MySQLConnector (MySQL连接器)                           │
│  ├── OracleConnector (Oracle连接器)                          │
│  ├── SqlServerConnector (SQL Server连接器)                   │
│  └── PostgreSQLConnector (PostgreSQL连接器)                 │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 核心接口设计

#### 3.2.1 设计原则

**SQL构建与参数填充分离**：
- **SQL构建**：负责生成SQL语句结构，不涉及运行时参数
- **参数填充**：负责处理执行时的参数，如游标、自定义参数等
- **职责清晰**：构建上下文只包含结构信息，参数上下文只包含运行时参数

**流式处理替代分页处理**：
- **流式查询**：使用JDBC流式结果集，避免内存溢出，提升大数据量处理性能
- **游标查询**：支持断点续传，基于主键值实现精确的断点定位
- **无需分页**：流式处理本身就是为了避免分页查询的性能问题而设计

#### 3.2.2 SqlTemplate接口

```java
/**
 * SQL模板接口
 * 提供统一的SQL模板定义和构建方法
 */
public interface SqlTemplate {
    
    /**
     * 获取数据库特定的引号字符
     */
    String getLeftQuotation();
    String getRightQuotation();
    
    
    /**
     * 获取数据库特定的游标SQL模板
     */
    String getCursorTemplate();
    
    /**
     * 获取数据库特定的流式查询SQL模板
     */
    String getStreamingTemplate();
    
    /**
     * 基础查询SQL模板
     */
    String getQueryTemplate();
    
    /**
     * 计数查询SQL模板
     */
    String getCountTemplate();
    
    /**
     * 插入SQL模板
     */
    String getInsertTemplate();
    
    /**
     * 更新SQL模板
     */
    String getUpdateTemplate();
    
    /**
     * 删除SQL模板
     */
    String getDeleteTemplate();
    
    /**
     * 存在性检查SQL模板
     */
    String getExistTemplate();
    
    /**
     * 构建SQL（仅构建SQL结构，包含参数占位符）
     * @param templateType 模板类型
     * @param buildContext 构建上下文
     * @return 构建后的SQL（包含?占位符）
     */
    String buildSql(SqlTemplateType templateType, SqlBuildContext buildContext);
}
```

#### 3.2.3 SqlBuildContext构建上下文

```java
/**
 * SQL构建上下文
 * 包含构建SQL结构所需的所有参数（不包含运行时参数）
 */
public class SqlBuildContext {
    private String schema;
    private String tableName;
    private List<Field> fields;
    private List<String> primaryKeys;
    private String queryFilter;
    private String cursorCondition;
    private String orderByClause;
    
    // 构造方法和getter/setter
}
```

#### 3.2.4 简化的设计原则

**SQL模板只负责SQL结构构建，不处理参数**：
- SQL模板返回纯SQL字符串，包含参数占位符`?`
- 参数在SQL执行时通过`PreparedStatement`处理
- 保持与现有架构的一致性

#### 3.2.6 SqlTemplateType枚举

```java
/**
 * SQL模板类型枚举
 */
public enum SqlTemplateType {
    QUERY_STREAM("query_stream", "流式查询"),
    QUERY_CURSOR("query_cursor", "游标查询"),
    QUERY_COUNT("query_count", "计数查询"),
    QUERY_EXIST("query_exist", "存在性检查"),
    INSERT("insert", "插入"),
    UPDATE("update", "更新"),
    DELETE("delete", "删除");
    
    private final String code;
    private final String description;
}
```

### 3.3 默认模板实现

#### 3.3.1 DefaultSqlTemplate

```java
/**
 * 默认SQL模板实现
 * 提供适用于大多数数据库的通用SQL模板
 */
public class DefaultSqlTemplate implements SqlTemplate {
    
    @Override
    public String getLeftQuotation() {
        return "\"";
    }
    
    @Override
    public String getRightQuotation() {
        return "\"";
    }
    
    @Override
    public String getQueryTemplate() {
        return "SELECT {fields} FROM {schema}{table} {where}";
    }
    
    @Override
    public String getCountTemplate() {
        return "SELECT COUNT(1) FROM {schema}{table} {where}";
    }
    
    @Override
    public String getInsertTemplate() {
        return "INSERT INTO {schema}{table} ({fields}) VALUES ({placeholders})";
    }
    
    @Override
    public String getUpdateTemplate() {
        return "UPDATE {schema}{table} SET {setClause} WHERE {whereClause}";
    }
    
    @Override
    public String getDeleteTemplate() {
        return "DELETE FROM {schema}{table} WHERE {whereClause}";
    }
    
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} LIMIT ? OFFSET ?";
    }
    
    @Override
    public String getStreamingTemplate() {
        return "{baseQuery} {where}";
    }
    
    @Override
    public String getExistTemplate() {
        return "SELECT 1 FROM {schema}{table} WHERE {whereClause} LIMIT 1";
    }
    
    @Override
    public String buildSql(SqlTemplateType templateType, SqlBuildContext buildContext) {
        String template = getTemplateByType(templateType);
        return processTemplate(template, buildContext);
    }
    
    private String processTemplate(String template, SqlBuildContext buildContext) {
        // 模板变量替换逻辑
        return template
            .replace("{schema}", buildContext.getSchema())
            .replace("{table}", buildContext.getTableName())
            .replace("{fields}", buildFieldList(buildContext.getFields()))
            .replace("{where}", buildWhereClause(buildContext))
            .replace("{setClause}", buildSetClause(buildContext))
            .replace("{whereClause}", buildWhereClause(buildContext))
            .replace("{placeholders}", buildPlaceholders(buildContext.getFields().size()))
            .replace("{orderBy}", buildOrderByClause(buildContext.getPrimaryKeys()));
    }
    
}
```

#### 3.3.3 使用示例

```java
/**
 * 简化的SQL模板使用示例
 */
public class SqlTemplateUsageExample {
    
    public void buildStreamQuery() {
        // 构建SQL结构
        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema("public");
        buildContext.setTableName("users");
        buildContext.setFields(Arrays.asList(new Field("id"), new Field("name")));
        buildContext.setQueryFilter("WHERE status = ?");  // 过滤条件
        
        SqlTemplate template = new MySQLTemplate();
        String sql = template.buildSql(SqlTemplateType.QUERY_STREAM, buildContext);
        // 结果：SQL = "SELECT id, name FROM public.users WHERE status = ?"
        
        // 参数在SQL执行时通过PreparedStatement处理
        // 例如：preparedStatement.setString(1, "active");
    }
    
    public void buildQueryWithCursor() {
        // 构建SQL结构
        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema("public");
        buildContext.setTableName("users");
        buildContext.setFields(Arrays.asList(new Field("id"), new Field("name")));
        buildContext.setCursorCondition("id > ?");  // 游标条件
        buildContext.setPrimaryKeys(Arrays.asList("id"));
        
        SqlTemplate template = new MySQLTemplate();
        String sql = template.buildSql(SqlTemplateType.QUERY_CURSOR, buildContext);
        // 结果：SQL = "SELECT id, name FROM public.users WHERE id > ? ORDER BY id LIMIT ?"
        
        // 参数在SQL执行时通过PreparedStatement处理
        // 例如：preparedStatement.setLong(1, 100L);  // 游标值
        //      preparedStatement.setInt(2, 1000);    // LIMIT值
    }
}
```

### 3.4 数据库特定模板实现

#### 3.4.1 MySQL模板实现

```java
/**
 * MySQL特定SQL模板实现
 */
public class MySQLTemplate extends DefaultSqlTemplate {
    
    @Override
    public String getLeftQuotation() {
        return "`";
    }
    
    @Override
    public String getRightQuotation() {
        return "`";
    }
    
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} LIMIT ?, ?";
    }
}
```

#### 3.4.2 Oracle模板实现

```java
/**
 * Oracle特定SQL模板实现
 */
public class OracleTemplate extends DefaultSqlTemplate {
    
    
    @Override
    public String getCursorTemplate() {
        return "SELECT * FROM (SELECT ROWNUM rn, t.* FROM ({baseQuery} {where} ORDER BY {orderBy}) t WHERE ROWNUM <= ?) WHERE rn > ?";
    }
}
```

#### 3.4.3 SQL Server模板实现

```java
/**
 * SQL Server特定SQL模板实现
 */
public class SqlServerTemplate extends DefaultSqlTemplate {
    
    @Override
    public String getLeftQuotation() {
        return "[";
    }
    
    @Override
    public String getRightQuotation() {
        return "]";
    }
    
    
    @Override
    public String getCursorTemplate() {
        return "{baseQuery} {where} ORDER BY {orderBy} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
    }
}
```

### 3.5 连接器模板属性设计

每个数据库连接器直接声明SQL模板属性，简化架构设计：

```java
/**
 * 抽象数据库连接器基类
 * 提供SQL模板支持
 */
public abstract class AbstractDatabaseConnector {
    
    /**
     * SQL模板实例
     * 每个连接器实例化时创建对应的模板
     */
    protected final SqlTemplate sqlTemplate;
    
    protected AbstractDatabaseConnector() {
        this.sqlTemplate = createSqlTemplate();
    }
    
    /**
     * 创建SQL模板实例
     * 子类可以覆盖此方法提供特定的模板实现
     */
    protected SqlTemplate createSqlTemplate() {
        return new DefaultSqlTemplate();
    }
    
    /**
     * 获取SQL模板
     */
    protected SqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }
}
```

### 3.6 具体连接器实现

#### 3.6.1 MySQL连接器示例

```java
/**
 * MySQL连接器
 * 使用MySQL特定的SQL模板
 */
public class MySQLConnector extends AbstractDatabaseConnector {
    
    @Override
    protected SqlTemplate createSqlTemplate() {
        return new MySQLTemplate();
    }
    
    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> commands = new HashMap<>();
        
        // 创建构建上下文
        SqlBuildContext context = createBuildContext(commandConfig);
        
        // 构建流式查询SQL
        String streamingSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_STREAM, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);
        
        // 构建游标查询SQL
        if (PrimaryKeyUtil.isSupportedCursor(commandConfig.getTable().getColumn())) {
            String cursorSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_CURSOR, context);
            commands.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }
        
        // 构建计数SQL
        String countSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_COUNT, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);
        
        return commands;
    }
    
    private SqlBuildContext createBuildContext(CommandConfig commandConfig) {
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();
        
        SqlBuildContext buildContext = new SqlBuildContext();
        buildContext.setSchema(getSchemaWithQuotation(dbConfig));
        buildContext.setTableName(QUOTATION + buildTableName(table.getName()) + QUOTATION);
        buildContext.setFields(table.getColumn());
        buildContext.setPrimaryKeys(PrimaryKeyUtil.findTablePrimaryKeys(table));
        buildContext.setQueryFilter(getQueryFilterSql(commandConfig));
        
        return buildContext;
    }
}
```

#### 3.6.2 Oracle连接器示例

```java
/**
 * Oracle连接器
 * 使用Oracle特定的SQL模板
 */
public class OracleConnector extends AbstractDatabaseConnector {
    
    @Override
    protected SqlTemplate createSqlTemplate() {
        return new OracleTemplate();
    }
    
    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> commands = new HashMap<>();
        
        // 创建构建上下文
        SqlBuildContext context = createBuildContext(commandConfig);
        
        // 构建流式查询SQL
        String streamingSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_STREAM, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);
        
        // 构建游标查询SQL
        if (PrimaryKeyUtil.isSupportedCursor(commandConfig.getTable().getColumn())) {
            String cursorSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_CURSOR, context);
            commands.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }
        
        // 构建计数SQL
        String countSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_COUNT, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);
        
        return commands;
    }
    
    private SqlBuildContext createBuildContext(CommandConfig commandConfig) {
        // Oracle特定的上下文构建逻辑
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();
        
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema(getSchemaWithQuotation(dbConfig));
        context.setTableName(QUOTATION + buildTableName(table.getName()) + QUOTATION);
        context.setFields(table.getColumn());
        context.setPrimaryKeys(PrimaryKeyUtil.findTablePrimaryKeys(table));
        context.setQueryFilter(getQueryFilterSql(commandConfig));
        
        return context;
    }
}
```

#### 3.6.3 SQL Server连接器示例

```java
/**
 * SQL Server连接器
 * 使用SQL Server特定的SQL模板
 */
public class SqlServerConnector extends AbstractDatabaseConnector {
    
    @Override
    protected SqlTemplate createSqlTemplate() {
        return new SqlServerTemplate();
    }
    
    @Override
    protected Map<String, String> buildSourceCommands(CommandConfig commandConfig) {
        Map<String, String> commands = new HashMap<>();
        
        // 创建构建上下文
        SqlBuildContext context = createBuildContext(commandConfig);
        
        // 构建流式查询SQL
        String streamingSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_STREAM, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_STREAM, streamingSql);
        
        // 构建游标查询SQL
        if (PrimaryKeyUtil.isSupportedCursor(commandConfig.getTable().getColumn())) {
            String cursorSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_CURSOR, context);
            commands.put(ConnectorConstant.OPERTION_QUERY_CURSOR, cursorSql);
        }
        
        // 构建计数SQL
        String countSql = sqlTemplate.buildSql(SqlTemplateType.QUERY_COUNT, context);
        commands.put(ConnectorConstant.OPERTION_QUERY_COUNT, countSql);
        
        return commands;
    }
    
    private SqlBuildContext createBuildContext(CommandConfig commandConfig) {
        // SQL Server特定的上下文构建逻辑
        Table table = commandConfig.getTable();
        DatabaseConfig dbConfig = (DatabaseConfig) commandConfig.getConnectorConfig();
        
        SqlBuildContext context = new SqlBuildContext();
        context.setSchema(getSchemaWithQuotation(dbConfig));
        context.setTableName(QUOTATION + buildTableName(table.getName()) + "]");
        context.setFields(table.getColumn());
        context.setPrimaryKeys(PrimaryKeyUtil.findTablePrimaryKeys(table));
        context.setQueryFilter(getQueryFilterSql(commandConfig));
        
        return context;
    }
}
```

## 4. 实施计划

### 4.1 实施阶段

#### 阶段一：核心接口开发（1-2周）
1. 创建`SqlTemplate`接口和基础实现
2. 实现`SqlBuildContext`构建上下文
3. 实现默认SQL模板
4. 修改`AbstractDatabaseConnector`基类添加模板支持

#### 阶段二：数据库特定模板（2-3周）
1. 实现MySQL、Oracle、SQL Server、PostgreSQL、SQLite特定模板
2. 测试各数据库模板的正确性
3. 优化模板性能和可读性

#### 阶段三：连接器集成（2-3周）
1. 修改现有连接器继承新的基类
2. 为每个连接器实现`createSqlTemplate()`方法
3. 重构`buildSourceCommands`方法使用模板系统
4. 保持向后兼容性

#### 阶段四：测试和优化（1-2周）
1. 全面测试所有连接器的SQL生成
2. 性能测试和优化
3. 文档完善
4. 代码审查和重构

