package org.dbsyncer.parser.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.convert.Handler;
import org.dbsyncer.parser.convert.handler.*;

/**
 * 支持的转换类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public enum ConvertEnum {

    /**
     * 默认值
     */
    DEFAULT("DEFAULT", "默认值", 1,
        "字段值为 null 或空时使用默认值",
        "参数：ACTIVE<br>原值：null → 结果：ACTIVE",
        new DefaultHandler()),
    /**
     * 系统时间戳
     */
    SYSTEM_TIMESTAMP("SYSTEM_TIMESTAMP", "系统时间戳", 0,
        "生成当前系统时间戳",
        "原值：任意 → 结果：2025-01-XX 10:00:00",
        new TimestampHandler()),
    /**
     * 系统日期Date
     */
    SYSTEM_DATE("SYSTEM_DATE", "系统日期", 0,
        "生成当前系统日期",
        "原值：任意 → 结果：2025-01-XX",
        new DateHandler()),
    /**
     * Timestamp转Date
     */
    TIMESTAMP_TO_DATE("TIMESTAMP_TO_DATE", "Timestamp转Date", 0,
        "将 Timestamp 转换为 Date",
        "原值：Timestamp → 结果：Date",
        new TimestampToDateHandler()),
    /**
     * Timestamp转中国标准时间
     */
    TIMESTAMP_TO_CHINESE_STANDARD_TIME("TIMESTAMP_TO_CHINESE_STANDARD_TIME", "Timestamp转yyyy-MM-dd HH:mm:ss", 0,
        "将 Timestamp 转换为字符串格式",
        "原值：Timestamp → 结果：2025-01-XX 10:00:00",
        new TimestampToChineseStandardTimeHandler()),
    /**
     * Timestamp转Long
     */
    TIMESTAMP_TO_LONG("TIMESTAMP_TO_LONG", "Timestamp转Long", 0,
        "将 Timestamp 转换为 Long（毫秒数）",
        "原值：Timestamp → 结果：1704067200000",
        new TimestampToLongHandler()),
    /**
     * Long转Timestamp
     */
    LONG_TO_TIMESTAMP("LONG_TO_TIMESTAMP", "Long转Timestamp", 0,
        "将 Long（毫秒数）转换为 Timestamp",
        "原值：1704067200000 → 结果：Timestamp",
        new LongToTimestampHandler()),
    /**
     * String转Timestamp
     */
    STRING_TO_TIMESTAMP("STRING_TO_TIMESTAMP", "String转Timestamp", 0,
        "将字符串转换为 Timestamp",
        "原值：2025-01-01 10:00:00 → 结果：Timestamp",
        new StringToTimestampHandler()),
    /**
     * String转日期自定义格式
     */
    STRING_TO_FORMAT_DATE("STRING_TO_FORMAT_DATE", "String转Date自定义格式", 1,
        "将字符串按指定格式转换为 Date",
        "参数：yyyy-MM-dd<br>原值：2025-01-01 → 结果：Date",
        new StringToFormatDateHandler()),
    /**
     * Number转String
     */
    NUMBER_TO_STRING("NUMBER_TO_STRING", "Number转String", 0,
        "将数字转换为字符串",
        "原值：123 → 结果：\"123\"",
        new NumberToStringHandler()),
    /**
     * Byte[]转String
     */
    BYTES_TO_STRING("BYTES_TO_STRING", "Byte[]转String", 0,
        "将字节数组转换为字符串",
        "原值：byte[] → 结果：字符串",
        new BytesToStringHandler()),
    /**
     * 替换
     */
    REPLACE("REPLACE", "替换", 2,
        "替换字符串中的指定内容",
        "参数：old,new<br>原值：hello world → 结果：hello new",
        new ReplaceHandler()),
    /**
     * 追加在前面,例如"张三"追加123 => 123张三
     */
    PREPEND("PREPEND", "前面追加", 1,
        "在字段值前面追加指定字符串",
        "参数：PREFIX_<br>原值：123 → 结果：PREFIX_123",
        new PrependHandler()),
    /**
     * 追加在后面,例如"张三"追加123 => 张三123
     */
    APPEND("APPEND", "后面追加", 1,
        "在字段值后面追加指定字符串",
        "参数：_SUFFIX<br>原值：123 → 结果：123_SUFFIX",
        new AppendHandler()),
    /**
     * AES加密
     */
    AES_ENCRYPT("AES_ENCRYPT", "AES加密", 1,
        "使用 AES 算法加密字段值",
        "参数：encryption_key<br>原值：张三 → 结果：加密后的字符串",
        new AesEncryptHandler()),
    /**
     * AES解密
     */
    AES_DECRYPT("AES_DECRYPT", "AES解密", 1,
        "使用 AES 算法解密字段值",
        "参数：encryption_key<br>原值：加密字符串 → 结果：张三",
        new AesDecryptHandler()),
    /**
     * SHA1加密
     */
    SHA1("SHA1", "SHA1加密", 0,
        "使用 SHA1 算法加密字段值",
        "原值：password → 结果：5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8",
        new Sha1Handler()),
    /**
     * UUID
     */
    UUID("UUID", "UUID", 0,
        "生成 UUID 字符串",
        "原值：任意 → 结果：550e8400-e29b-41d4-a716-446655440000",
        new UUIDHandler()),
    /**
     * 去掉首字符
     */
    REM_STR_FIRST("REM_STR_FIRST", "去掉首字符", 0,
        "去掉字符串的第一个字符",
        "原值：12345 → 结果：2345",
        new RemStrFirstHandler()),
    /**
     * 去掉尾字符
     */
    REM_STR_LAST("REM_STR_LAST", "去掉尾字符", 0,
        "去掉字符串的最后一个字符",
        "原值：12345 → 结果：1234",
        new RemStrLastHandler()),
    /**
     * 从前面截取N个字符
     */
    SUB_STR_FIRST("SUB_STR_FIRST", "从前面截取N个字符", 1,
        "从字符串前面截取指定数量的字符",
        "参数：3<br>原值：12345 → 结果：123",
        new SubStrFirstHandler()),
    /**
     * 从后面截取N个字符
     */
    SUB_STR_LAST("SUB_STR_LAST", "从后面截取N个字符", 1,
        "从字符串后面截取指定数量的字符",
        "参数：3<br>原值：12345 → 结果：345",
        new SubStrLastHandler()),
    /**
     * 清空
     */
    CLEAR("CLEAR", "清空", 0,
        "将字段值清空",
        "原值：任意值 → 结果：空字符串",
        new ClearHandler()),
    /**
     * 表达式规则
     */
    EXPRESSION("EXPRESSION", "表达式", -1,
        "使用表达式计算字段值，支持字段引用、字符串拼接、数学运算等",
        "表达式：${first_name} + ' ' + ${last_name}<br>结果：张 三",
        new ExpressionHandler()),
    /**
     * 固定值规则
     */
    FIXED("FIXED", "固定值", -1,
        "使用固定值作为字段值",
        "表达式：BATCH_001<br>结果：BATCH_001",
        new FixedHandler());

    // 转换编码
    private final String code;
    // 转换名称
    private final String name;
    // 参数个数
    private final int argNum;
    // 说明
    private final String description;
    // 示例（支持 HTML，使用 <br> 换行）
    private final String example;
    // 转换实现
    private final Handler handler;

    ConvertEnum(String code, String name, int argNum, String description, String example, Handler handler) {
        this.code = code;
        this.name = name;
        this.argNum = argNum;
        this.description = description;
        this.example = example;
        this.handler = handler;
    }

    public static Handler getHandler(String code) throws ParserException {
        for (ConvertEnum e : ConvertEnum.values()) {
            if (StringUtil.equals(code, e.getCode())) {
                return e.getHandler();
            }
        }
        throw new ParserException(String.format("Handler code \"%s\" does not exist.", code));
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public int getArgNum() {
        return argNum;
    }

    public String getDescription() {
        return description;
    }

    public String getExample() {
        return example;
    }

    public Handler getHandler() {
        return handler;
    }

}