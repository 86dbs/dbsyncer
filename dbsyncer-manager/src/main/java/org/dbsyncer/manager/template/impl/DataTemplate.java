package org.dbsyncer.manager.template.impl;

import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 同步数据和日志模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/5/20 18:59
 */
@Component
public final class DataTemplate {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    public List<Map> query(Query query) {
        return storageService.query(query);
    }

    public void clear(StorageEnum type, String collectionId) {
        storageService.clear(type, collectionId);
    }
}