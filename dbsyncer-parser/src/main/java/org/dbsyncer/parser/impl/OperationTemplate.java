/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 通用配置存储模板（无领域编排；任务见 {@link org.dbsyncer.parser.TaskProfile}，
 * 用户/连接器/表映射/Meta 见各自 Profile）。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class OperationTemplate {

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    public <T> T queryObject(Class<T> clazz, String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        StorageEnum type = ConfigModelUtil.getStorageEnum(newInstanceType(clazz));
        Query query = new Query();
        query.setType(type);
        query.setPageNum(1);
        query.setPageSize(1);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, id);
        Paging paging = storageService.query(query);
        List<Map> data = (List<Map>) paging.getData();
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        return parseRow(data.get(0), clazz);
    }

    public String execute(OperationConfig config) {
        ConfigModel model = config.getModel();
        Assert.notNull(model, "ConfigModel can not be null.");
        CommandEnum cmd = config.getCommandEnum();
        Assert.notNull(cmd, "CommandEnum can not be null.");
        Assert.isTrue(!(model instanceof UserConfig), "UserConfig must go through UserProfile.syncUserConfig");
        if (CommandEnum.OPR_ADD == cmd) {
            if (StringUtil.isBlank(model.getId())) {
                model.setId(String.valueOf(snowflakeIdWorker.nextId()));
            }
        }

        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        StorageEnum type = ConfigModelUtil.getStorageEnum(model.getType());
        if (CommandEnum.OPR_EDIT == cmd) {
            storageService.edit(type, params);
        } else {
            storageService.add(type, params);
        }
        return model.getId();
    }

    /**
     * 批量添加配置：单次存储批量写入。
     */
    public List<String> executeBatch(List<? extends ConfigModel> models, CommandEnum commandEnum) {
        if (CollectionUtils.isEmpty(models)) {
            return Collections.emptyList();
        }
        Assert.notNull(commandEnum, "CommandEnum can not be null.");
        Assert.isTrue(commandEnum == CommandEnum.OPR_ADD, "Batch execute only supports OPR_ADD");

        List<Map> paramsList = new ArrayList<>(models.size());
        for (ConfigModel model : models) {
            Assert.notNull(model, "ConfigModel can not be null.");
            if (StringUtil.isBlank(model.getId())) {
                model.setId(String.valueOf(snowflakeIdWorker.nextId()));
            }
            paramsList.add(ConfigModelUtil.convertModelToMap(model));
        }
        StorageEnum type = ConfigModelUtil.getStorageEnum(models.get(0).getType());
        storageService.addBatch(type, null, paramsList);
        return models.stream().map(ConfigModel::getId).collect(Collectors.toList());
    }

    public void remove(OperationConfig config) {
        String id = config.getId();
        Assert.hasText(id, "ID can not be empty.");
        storageService.remove(StorageEnum.CONFIG, id);
        storageService.remove(StorageEnum.USER, id);
        storageService.remove(StorageEnum.CONNECTOR, id);
        storageService.remove(StorageEnum.TASK, id);
        storageService.remove(StorageEnum.META, id);
    }

    /**
     * 按存储类型统计行数（仅 total，不拉明细）。
     */
    public int count(StorageEnum type, Query condition) {
        Query query = new Query();
        query.setType(type);
        query.setQueryTotal(true);
        query.setPageNum(1);
        query.setPageSize(1);
        if (condition != null) {
            query.setBooleanFilter(condition.getBooleanFilter());
        }
        Paging paging = storageService.query(query);
        return paging == null ? 0 : (int) paging.getTotal();
    }

    /**
     * 分页查询指定存储表，反序列化 json 列为模型。
     */
    public <T> List<T> queryList(StorageEnum type, Query condition, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        Query query = new Query();
        query.setType(type);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        if (condition != null) {
            query.setBooleanFilter(condition.getBooleanFilter());
        }
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                T model = parseRow(row, clazz);
                if (model != null) {
                    result.add(model);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    private String newInstanceType(Class<?> clazz) {
        try {
            ConfigModel model = (ConfigModel) clazz.newInstance();
            return model.getType();
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    private <T> T parseRow(Map row, Class<T> clazz) {
        return ConfigModelUtil.parseFromRow(row, clazz);
    }
}
