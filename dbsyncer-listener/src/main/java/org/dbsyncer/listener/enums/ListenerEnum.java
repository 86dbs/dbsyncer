package org.dbsyncer.listener.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.config.MysqlListenerConfig;
import org.dbsyncer.listener.config.TimingListenerConfig;

/**
 * 支持监听器类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:56
 */
public enum ListenerEnum {

    /**
     * Mysql
     */
    MYSQL("Mysql", MysqlListenerConfig.class),
    /**
     * 轮询
     */
    POLLING("Polling", TimingListenerConfig.class);

    // 策略名称
    private String type;

    // 配置
    private Class<?> configClass;

    ListenerEnum(String type, Class<?> configClass) {
        this.type = type;
        this.configClass = configClass;
    }

    public static Class<?> getConfigClass(String type) throws ListenerException {
        for (ListenerEnum e : ListenerEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getConfigClass();
            }
        }
        throw new ListenerException(String.format("Listener type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Class<?> getConfigClass() {
        return configClass;
    }

}
