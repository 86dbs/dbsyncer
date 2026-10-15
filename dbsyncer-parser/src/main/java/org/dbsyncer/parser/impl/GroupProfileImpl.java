/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.GroupProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link GroupProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
@Component
public class GroupProfileImpl extends AbstractConfigModelProfile<Group> implements GroupProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public Group getGroup(String id) {
        return getCache(id);
    }

    @Override
    public List<Group> listGroups() {
        Query query = new Query();
        query.setType(StorageEnum.GROUP);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        query.addOrderBy(ConfigConstant.GROUP_SORT, SortEnum.ASC);
        query.addOrderBy(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortEnum.DESC);
        List<Group> result = new ArrayList<>();
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            for (Object item : paging.getData()) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Group group = ConfigModelUtil.parseFromRow((Map) item, Group.class);
                if (group != null) {
                    result.add(group);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    @Override
    public Group getByName(String name) {
        if (StringUtil.isBlank(name)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.GROUP);
        query.addFilter(ConfigConstant.CONFIG_MODEL_NAME, name);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object first = paging.getData().iterator().next();
        if (!(first instanceof Map)) {
            return null;
        }
        return ConfigModelUtil.parseFromRow((Map) first, Group.class);
    }

    @Override
    public String addGroup(Group group) {
        String id = operationTemplate.execute(group, CommandEnum.OPR_ADD);
        removeCache(id);
        return id;
    }

    @Override
    public String updateGroup(Group group) {
        String id = operationTemplate.execute(group, CommandEnum.OPR_EDIT);
        removeCacheAndNotice(id);
        return id;
    }

    @Override
    public void removeGroup(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.GROUP, id);
        removeCacheAndNotice(id);
    }
    
}
