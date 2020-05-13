package org.dbsyncer.manager.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.listener.Extractor;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.puller.Increment;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum IncrementEnum {

    /**
     * 日志
     */
    LOG("log", new Increment() {

        @Override
        public void execute(Extractor extractor) {
            extractor.extract();
        }
    }),
    /**
     * 定时
     */
    TIMING("timing", new Increment() {

        @Override
        public void execute(Extractor extractor) {
            extractor.extractTiming();
        }
    });

    private String    type;
    private Increment increment;

    IncrementEnum(String type, Increment increment) {
        this.type = type;
        this.increment = increment;
    }

    public static Increment getIncrement(String type) throws ManagerException {
        for (IncrementEnum e : IncrementEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getIncrement();
            }
        }
        throw new ManagerException(String.format("Increment type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Increment getIncrement() {
        return increment;
    }
}