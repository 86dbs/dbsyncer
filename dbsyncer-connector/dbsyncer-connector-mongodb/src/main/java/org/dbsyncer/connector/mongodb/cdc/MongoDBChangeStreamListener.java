/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.cdc;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonDocument;
import org.bson.Document;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mongodb.MongoConnectorInstance;
import org.dbsyncer.connector.mongodb.MongoDBException;
import org.dbsyncer.connector.mongodb.config.MongoDBConfig;
import org.dbsyncer.connector.mongodb.constant.MongoDBConstant;
import org.dbsyncer.connector.mongodb.util.MongoUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MongoDB Change Stream 实时增量监听器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:30
 */
public final class MongoDBChangeStreamListener extends AbstractListener<MongoConnectorInstance> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<CollectionWatcher> watchers = new ArrayList<>();
    private volatile boolean connected;
    private Worker worker;

    @Override
    public void start() {
        if (CollectionUtils.isEmpty(filterTable)) {
            throw new MongoDBException("Change Stream 监听集合不能为空");
        }
        MongoConnectorInstance instance = getConnectorInstance();
        String databaseName = resolveDatabaseName();
        try {
            for (String collectionName : filterTable) {
                Table table = findSourceTable(collectionName);
                if (table == null) {
                    logger.warn("未找到集合 {} 的表映射，跳过 Change Stream 订阅", collectionName);
                    continue;
                }
                CollectionWatcher watcher = new CollectionWatcher(databaseName, collectionName, table);
                watcher.open(instance.getConnection(), snapshot);
                watchers.add(watcher);
                logger.info("MongoDB Change Stream 已订阅库 {} 集合 {}", databaseName, collectionName);
            }
            if (watchers.isEmpty()) {
                throw new MongoDBException("没有可订阅的 MongoDB 集合");
            }
            connected = true;
            worker = new Worker();
            worker.setName("mongodb-cs-listener-" + instance.getConfig().getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动 MongoDB Change Stream 监听器失败", e);
            errorEvent(e);
            close();
            throw new MongoDBException("启动 MongoDB Change Stream 监听器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (!connected && watchers.isEmpty()) {
            return;
        }
        connected = false;
        if (worker != null && !worker.isInterrupted()) {
            worker.interrupt();
        }
        if (worker != null) {
            try {
                worker.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            worker = null;
        }
        watchers.forEach(CollectionWatcher::close);
        watchers.clear();
        forceFlushEvent();
    }

    @Override
    public Map<String, String> captureSnapshot() {
        if (CollectionUtils.isEmpty(filterTable)) {
            return Collections.emptyMap();
        }
        MongoConnectorInstance instance = getConnectorInstance();
        String databaseName = resolveDatabaseName();
        Map<String, String> captured = new HashMap<>();
        for (String collectionName : filterTable) {
            if (findSourceTable(collectionName) == null) {
                continue;
            }
            try {
                String resumeToken = openResumeToken(instance.getConnection(), databaseName, collectionName);
                if (StringUtil.isNotBlank(resumeToken)) {
                    String key = resumeKey(collectionName);
                    captured.put(key, resumeToken);
                    snapshot.put(key, resumeToken);
                }
            } catch (Exception e) {
                logger.error("捕获 MongoDB 集合 {} resume token 失败: {}", collectionName, e.getMessage(), e);
            }
        }
        if (!captured.isEmpty()) {
            forceFlushEvent();
        }
        return captured;
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (offset == null || StringUtil.isBlank(offset.getNextFileName()) || offset.getPosition() == null) {
            return;
        }
        snapshot.put(resumeKey(offset.getNextFileName()), String.valueOf(offset.getPosition()));
    }

    private String openResumeToken(MongoClient client, String databaseName, String collectionName) {
        MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(collectionName);
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor()) {
            BsonDocument resumeToken = cursor.getResumeToken();
            return resumeToken == null ? null : resumeToken.toJson();
        } catch (MongoCommandException e) {
            throw new MongoDBException("Change Stream 需要 MongoDB 副本集或分片集群: " + e.getMessage(), e);
        }
    }

    private Table findSourceTable(String collectionName) {
        if (CollectionUtils.isEmpty(sourceTable)) {
            return null;
        }
        for (Table table : sourceTable) {
            if (StringUtil.equals(table.getName(), collectionName)) {
                return table;
            }
        }
        return null;
    }

    private String resolveDatabaseName() {
        if (StringUtil.isNotBlank(database)) {
            return database.trim();
        }
        if (connectorConfig instanceof MongoDBConfig) {
            return ((MongoDBConfig) connectorConfig).getDatabase();
        }
        throw new MongoDBException("MongoDB 数据库名不能为空");
    }

    private String resumeKey(String collectionName) {
        return MongoDBConstant.RESUME_TOKEN_PREFIX + collectionName;
    }

    private void trySendEvent(ChangedEvent event) {
        while (connected) {
            try {
                changeEvent(event);
                break;
            } catch (QueueOverflowException e) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void processChange(CollectionWatcher watcher, ChangeStreamDocument<Document> change) {
        String operation = mapOperation(change.getOperationType().getValue());
        if (operation == null) {
            return;
        }
        Document document = change.getFullDocument();
        if (document == null && change.getDocumentKey() != null) {
            document = Document.parse(change.getDocumentKey().toJson());
        }
        if (document == null) {
            return;
        }
        Map<String, Object> rowMap = MongoUtil.toMap(document);
        List<Object> rowData = mapToRowList(watcher.table.getColumn(), rowMap);
        BsonDocument resumeToken = change.getResumeToken();
        String resumeTokenJson = resumeToken.toJson();
        if (StringUtil.isNotBlank(resumeTokenJson)) {
            snapshot.put(resumeKey(watcher.collectionName), resumeTokenJson);
        }
        RowChangedEvent event = new RowChangedEvent(watcher.table.getName(), operation, rowData,
                watcher.collectionName, resumeTokenJson);
        trySendEvent(event);
    }

    private List<Object> mapToRowList(List<Field> columns, Map<String, Object> rowMap) {
        if (CollectionUtils.isEmpty(columns) || rowMap == null) {
            return new ArrayList<>();
        }
        return columns.stream().map(field -> rowMap.get(field.getName())).collect(Collectors.toList());
    }

    private String mapOperation(String operationType) {
        if (StringUtil.isBlank(operationType)) {
            return null;
        }
        switch (operationType) {
            case "insert":
                return ConnectorConstant.OPERTION_INSERT;
            case "update":
            case "replace":
                return ConnectorConstant.OPERTION_UPDATE;
            case "delete":
                return ConnectorConstant.OPERTION_DELETE;
            default:
                return null;
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    boolean processed = false;
                    for (CollectionWatcher watcher : watchers) {
                        if (pollChange(watcher)) {
                            processed = true;
                        }
                    }
                    if (!processed) {
                        sleepInMills(10L);
                    }
                } catch (Exception e) {
                    if (isInterrupted() || !connected) {
                        break;
                    }
                    logger.error(e.getMessage(), e);
                    errorEvent(e);
                    sleepInMills(1000L);
                }
            }
            logger.info("MongoDB Change Stream Worker 线程已退出");
        }

        private boolean pollChange(CollectionWatcher watcher) {
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watcher.cursor;
            if (cursor == null) {
                return false;
            }
            ChangeStreamDocument<Document> change = cursor.tryNext();
            if (change == null) {
                return false;
            }
            processChange(watcher, change);
            return true;
        }
    }

    static final class CollectionWatcher {

        private final String databaseName;
        private final String collectionName;
        private final Table table;
        private MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;

        CollectionWatcher(String databaseName, String collectionName, Table table) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.table = table;
        }

        void open(MongoClient client, Map<String, String> snapshot) {
            MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(collectionName);
            try {
                com.mongodb.client.ChangeStreamIterable<Document> iterable = collection.watch()
                        .fullDocument(FullDocument.UPDATE_LOOKUP)
                        .batchSize(100);
                String resumeToken = snapshot.get(MongoDBConstant.RESUME_TOKEN_PREFIX + collectionName);
                if (StringUtil.isNotBlank(resumeToken)) {
                    iterable = iterable.resumeAfter(BsonDocument.parse(resumeToken));
                }
                cursor = iterable.cursor();
            } catch (MongoCommandException e) {
                throw new MongoDBException("Change Stream 需要 MongoDB 副本集或分片集群: " + e.getMessage(), e);
            }
        }

        void close() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }
    }
}
