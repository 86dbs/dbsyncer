package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConvertService;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/14 0:03
 */
@Component
public class ConvertServiceImpl implements ConvertService {

    @Autowired
    private Manager manager;

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return manager.getConvertEnumAll();
    }

}