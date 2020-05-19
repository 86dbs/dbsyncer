package org.dbsyncer.storage.support;

import org.dbsyncer.storage.AbstractStorageService;
import org.dbsyncer.storage.query.Query;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:22
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.storage.support.mysql")
public class MysqlStorageServiceImpl extends AbstractStorageService {

    @Override
    public List<Map> select(String collectionId, Query query) {
        return null;
    }

    @Override
    public void insert(String collectionId, Map params) throws IOException {

    }

    @Override
    public void update(String collectionId, Map params) throws IOException {

    }

    @Override
    public void delete(String collectionId, String id) throws IOException {

    }

    @Override
    public void insertLog(String collectionId, Map<String, Object> params) throws IOException {

    }

    @Override
    public void insertData(String collectionId, List<Map> list) throws IOException {

    }
}