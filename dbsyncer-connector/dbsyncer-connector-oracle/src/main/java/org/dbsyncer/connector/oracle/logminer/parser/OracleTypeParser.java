package org.dbsyncer.connector.oracle.logminer.parser;

import java.math.BigInteger;
import java.sql.Types;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

/**
 * @Description :oracle解析到java类
 * @Param :
 * @return :
 * @author : life
 * @date : 2023/12/15  11:25
 */
public class OracleTypeParser {
    public static Object convertToJavaType(Field field,String value){
        if (value == null){
            return null;
        }
        switch (field.getType()) {
            case Types.DECIMAL:
                return new BigInteger(
                        StringUtil.replace(value, StringUtil.SINGLE_QUOTATION, StringUtil.EMPTY));
            case Types.TIMESTAMP:
                return DateFormatUtil.stringToTimestamp(StringUtil.replace(value,StringUtil.SINGLE_QUOTATION,StringUtil.EMPTY));
            default:
                return StringUtil.replace(value,StringUtil.SINGLE_QUOTATION,StringUtil.EMPTY);
        }
    }



}
