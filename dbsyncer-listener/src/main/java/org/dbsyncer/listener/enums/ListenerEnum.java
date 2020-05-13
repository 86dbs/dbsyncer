package org.dbsyncer.listener.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.listener.DefaultExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.extractor.MysqlExtractor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum ListenerEnum {

    /**
     * Mysql
     */
    MYSQL(ConnectorEnum.MYSQL.getType(), new MysqlExtractor()),
    ;

    private String type;
    private DefaultExtractor extractor;

    ListenerEnum(String type, DefaultExtractor extractor) {
        this.type = type;
        this.extractor = extractor;
    }

    /**
     * 获取抽取器
     *
     * @param type
     * @return
     * @throws ListenerException
     */
    public static DefaultExtractor getExtractor(String type) throws ListenerException {
        for (ListenerEnum e : ListenerEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getExtractor();
            }
        }
        throw new ListenerException(String.format("Extractor type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public DefaultExtractor getExtractor() {
        return extractor;
    }
}
