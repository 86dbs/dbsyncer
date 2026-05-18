package org.dbsyncer.storage.util;

import org.apache.lucene.document.*;
import org.apache.lucene.util.BytesRef;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * <b>索引维护工具类</b>
 * <p/>1、使用方法：
 * <p/> new IntPoint(name, value); 存放int类型
 * <p/> new StoredField(name, value); 要存储值，必须添加一个同名的StoredField
 * <p/> new NumericDocValuesField(name, value); 要排序，必须添加一个同名的SortedNumericDocValuesField
 * <p/> 其他FloatPoint、LongPoint、DoublePoint同上
 * <p/> id使用字符串，防止更新失败
 * <p>
 * <p/>2、Field：
 * <p/>IntPoint
 * <p/>FloatPoint
 * <p/>LongPoint
 * <p/>DoublePoint
 * <p/>BinaryPoint
 * <p/>StringField 索引不分词，所有的字符串会作为一个整体进行索引，例如通常用于country或id等
 * <p/>TextField 索引并分词，不包括term vectors，例如通常用于一个body Field
 * <p/>StoredField 存储Field的值，可以用 IndexSearcher.doc和IndexReader.document来获取存储的Field和存储的值
 * <p/>SortedDocValuesField 存储String、Text类型排序
 * <p/>NumericDocValuesField 存储Int、Long类型索引并排序，用于评分、排序和值检索
 * <p/>FloatDocValuesField 存储Float类型索引并排序
 * <p/>DoubleDocValuesField 存储Double类型索引并排序
 * <p/>BinaryDocValuesField 只存储不共享，例如标题类字段，如果需要共享并排序，推荐使用SortedDocValuesField
 * <p>
 * <p/>3、Lucene 6.0版本后：
 * <p>IntField 替换为 IntPoint</p>
 * <p>FloatField 替换为 FloatPoint</p>
 * <p>LongField 替换为 LongPoint</p>
 * <p>DoubleField 替换为 DoublePoint</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/19 22:07
 */
public abstract class DocumentUtil {

    private DocumentUtil() {
    }

    public static Document convertConfig2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = (String) params.get(ConfigConstant.CONFIG_MODEL_ID);
        String type = (String) params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String name = (String) params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String json = (String) params.get(ConfigConstant.CONFIG_MODEL_JSON);
        Long createTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        Long updateTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_UPDATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_TYPE, type, Field.Store.YES));
        doc.add(new TextField(ConfigConstant.CONFIG_MODEL_NAME, name, Field.Store.YES));
        // 配置信息
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_JSON, json));
        // 创建时间(不需要存储)
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        // 修改时间(不需要存储)
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        return doc;
    }

    public static Document convertLog2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = (String) params.get(ConfigConstant.CONFIG_MODEL_ID);
        String type = (String) params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String json = (String) params.get(ConfigConstant.CONFIG_MODEL_JSON);
        Long createTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_TYPE, type, Field.Store.YES));
        // 日志信息
        doc.add(new TextField(ConfigConstant.CONFIG_MODEL_JSON, json, Field.Store.YES));
        // 创建时间
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        return doc;
    }

    public static Document convertData2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = (String) params.get(ConfigConstant.CONFIG_MODEL_ID);
        Integer success = (Integer) params.get(ConfigConstant.DATA_SUCCESS);
        String tableGroupId = (String) params.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        String targetTableName = (String) params.get(ConfigConstant.DATA_TARGET_TABLE_NAME);
        String event = (String) params.get(ConfigConstant.DATA_EVENT);
        String error = (String) params.get(ConfigConstant.DATA_ERROR);
        Long createTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new IntPoint(ConfigConstant.DATA_SUCCESS, success));
        doc.add(new StoredField(ConfigConstant.DATA_SUCCESS, success));
        doc.add(new StringField(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableName, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.DATA_EVENT, event, Field.Store.YES));
        doc.add(new TextField(ConfigConstant.DATA_ERROR, error, Field.Store.YES));

        // 同步数据
        byte[] bytes = (byte[]) params.get(ConfigConstant.BINLOG_DATA);
        if (bytes == null) {
            bytes = new byte[0];
        }
        doc.add(new BinaryDocValuesField(ConfigConstant.BINLOG_DATA, new BytesRef(bytes)));
        doc.add(new StoredField(ConfigConstant.BINLOG_DATA, bytes));

        // 创建时间
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        return doc;
    }

    public static Document convertTask2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = getString(params, ConfigConstant.CONFIG_MODEL_ID);
        String name = getString(params, ConfigConstant.CONFIG_MODEL_NAME);
        Integer status = getInteger(params, ConfigConstant.TASK_STATUS);
        String type = getString(params, ConfigConstant.CONFIG_MODEL_TYPE);
        String json = getString(params, ConfigConstant.CONFIG_MODEL_JSON);
        Long createTime = getLong(params, ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        Long updateTime = getLong(params, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new TextField(ConfigConstant.CONFIG_MODEL_NAME, name, Field.Store.YES));
        doc.add(new IntPoint(ConfigConstant.TASK_STATUS, status));
        doc.add(new StoredField(ConfigConstant.TASK_STATUS, status));
        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_TYPE, type, Field.Store.YES));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_JSON, json));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        return doc;
    }

    public static Document convertValidateSyncDetail2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = getString(params, ConfigConstant.CONFIG_MODEL_ID);
        String taskId = getString(params, ConfigConstant.TASK_ID);
        String type = getString(params, ConfigConstant.CONFIG_MODEL_TYPE);
        String sourceTableName = getString(params, ConfigConstant.TASK_SOURCE_TABLE_NAME);
        String targetTableName = getString(params, ConfigConstant.DATA_TARGET_TABLE_NAME);
        Long sourceTotal = getLong(params, ConfigConstant.TASK_SOURCE_TOTAL);
        Long targetTotal = getLong(params, ConfigConstant.TASK_TARGET_TOTAL);
        Long diffTotal = getLong(params, ConfigConstant.TASK_DIFF_TOTAL);
        Long fixedTotal = getLong(params, ConfigConstant.TASK_FIXED_TOTAL);
        String content = getString(params, ConfigConstant.TASK_CONTENT);
        Long createTime = getLong(params, ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        Long updateTime = getLong(params, ConfigConstant.CONFIG_MODEL_UPDATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.TASK_ID, taskId, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_TYPE, type, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.TASK_SOURCE_TABLE_NAME, sourceTableName, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableName, Field.Store.YES));
        doc.add(new LongPoint(ConfigConstant.TASK_SOURCE_TOTAL, sourceTotal));
        doc.add(new StoredField(ConfigConstant.TASK_SOURCE_TOTAL, sourceTotal));
        doc.add(new NumericDocValuesField(ConfigConstant.TASK_SOURCE_TOTAL, sourceTotal));
        doc.add(new LongPoint(ConfigConstant.TASK_TARGET_TOTAL, targetTotal));
        doc.add(new StoredField(ConfigConstant.TASK_TARGET_TOTAL, targetTotal));
        doc.add(new NumericDocValuesField(ConfigConstant.TASK_TARGET_TOTAL, targetTotal));
        doc.add(new LongPoint(ConfigConstant.TASK_DIFF_TOTAL, diffTotal));
        doc.add(new StoredField(ConfigConstant.TASK_DIFF_TOTAL, diffTotal));
        doc.add(new NumericDocValuesField(ConfigConstant.TASK_DIFF_TOTAL, diffTotal));

        doc.add(new LongPoint(ConfigConstant.TASK_FIXED_TOTAL, fixedTotal));
        doc.add(new StoredField(ConfigConstant.TASK_FIXED_TOTAL, fixedTotal));
        doc.add(new NumericDocValuesField(ConfigConstant.TASK_FIXED_TOTAL, fixedTotal));
        doc.add(new TextField(ConfigConstant.TASK_CONTENT, content, Field.Store.YES));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new NumericDocValuesField(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        return doc;
    }

    private static String getString(Map params, String key) {
        Object value = params.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static Long getLong(Map params, String key) {
        Object value = params.get(key);
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static Integer getInteger(Map params, String key) {
        Object value = params.get(key);
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

}