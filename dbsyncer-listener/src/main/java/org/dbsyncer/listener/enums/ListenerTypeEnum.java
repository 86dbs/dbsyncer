package org.dbsyncer.listener.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.listener.Action;
import org.dbsyncer.listener.ListenerException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum ListenerTypeEnum {

    /**
     * 日志
     */
    LOG("log", extractor -> extractor.extract()),
    /**
     * 定时
     */
    TIMING("timing", extractor -> extractor.extractTiming());

    private String type;
    private Action action;

    ListenerTypeEnum(String type, Action action) {
        this.type = type;
        this.action = action;
    }

    public static Action getAction(String type) throws ListenerException {
        for (ListenerTypeEnum e : ListenerTypeEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getAction();
            }
        }
        throw new ListenerException(String.format("Action type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Action getAction() {
        return action;
    }
}