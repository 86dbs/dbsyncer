package org.dbsyncer.storage.util;

import org.apache.lucene.document.*;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/19 22:07
 */
public abstract class ParamsUtil {
    private ParamsUtil(){}

    public static Document convertParams2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = (String) params.get(ConfigConstant.CONFIG_MODEL_ID);
        String type = (String) params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String name = (String) params.get(ConfigConstant.CONFIG_MODEL_NAME);
        Long createTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        Long updateTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        String json = (String) params.get(ConfigConstant.CONFIG_MODEL_JSON);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_TYPE, type, Field.Store.YES));
        doc.add(new TextField(ConfigConstant.CONFIG_MODEL_NAME, name, Field.Store.YES));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, updateTime));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_JSON, json));
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
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_JSON, json));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        return doc;
    }

    public static Document convertData2Doc(Map params) {
        Assert.notNull(params, "Params can not be null.");
        Document doc = new Document();
        String id = (String) params.get(ConfigConstant.CONFIG_MODEL_ID);
        Boolean success = (Boolean) params.get(ConfigConstant.DATA_SUCCESS);
        String event = (String) params.get(ConfigConstant.DATA_EVENT);
        String error = (String) params.get(ConfigConstant.DATA_ERROR);
        String json = (String) params.get(ConfigConstant.CONFIG_MODEL_JSON);
        Long createTime = (Long) params.get(ConfigConstant.CONFIG_MODEL_CREATE_TIME);

        doc.add(new StringField(ConfigConstant.CONFIG_MODEL_ID, id, Field.Store.YES));
        doc.add(new StringField(ConfigConstant.DATA_SUCCESS, String.valueOf(success), Field.Store.YES));
        doc.add(new StringField(ConfigConstant.DATA_EVENT, event, Field.Store.YES));
        doc.add(new TextField(ConfigConstant.DATA_ERROR, error, Field.Store.YES));
        doc.add(new StoredField(ConfigConstant.CONFIG_MODEL_JSON, json));
        doc.add(new LongPoint(ConfigConstant.CONFIG_MODEL_CREATE_TIME, createTime));
        return doc;
    }
}