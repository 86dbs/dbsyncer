package org.dbsyncer.manager.extractor.increment;

import org.dbsyncer.manager.extractor.Increment;
import org.springframework.stereotype.Component;

/**
 * 定时同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/05/05 15:28
 */
@Component
public class TimingIncrement implements Increment {

    @Override
    public void close(String metaId) {

    }
}