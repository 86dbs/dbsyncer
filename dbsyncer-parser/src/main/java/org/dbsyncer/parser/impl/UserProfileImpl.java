/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link UserProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class UserProfileImpl implements UserProfile {

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public UserConfig getUserConfig() {
        List<UserInfo> users = listUserInfos();
        if (CollectionUtils.isEmpty(users)) {
            return null;
        }
        UserConfig config = new UserConfig();
        config.setName("用户配置");
        config.setUserInfoList(users);
        return config;
    }

    @Override
    public List<UserInfo> listUserInfos() {
        List<UserInfo> result = new ArrayList<>();
        Query query = new Query();
        query.setType(StorageEnum.USER);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                UserInfo user = ConfigModelUtil.parseFromRow(row, UserInfo.class);
                if (user != null) {
                    result.add(user);
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    @Override
    public String syncUserConfig(UserConfig config) {
        if (config == null) {
            return null;
        }
        List<UserInfo> users = config.getUserInfoList();
        if (CollectionUtils.isEmpty(users)) {
            return config.getId();
        }
        long now = System.currentTimeMillis();
        Map<String, UserInfo> existingByUsername = listUserInfos().stream()
                .collect(Collectors.toMap(UserInfo::getUsername, u -> u, (a, b) -> a));
        Set<String> keepUsernames = new HashSet<>();
        String firstId = null;
        for (UserInfo user : users) {
            keepUsernames.add(user.getUsername());
            UserInfo existing = existingByUsername.get(user.getUsername());
            if (existing != null) {
                user.setId(existing.getId());
                user.setCreateTime(existing.getCreateTime());
            } else if (StringUtil.isBlank(user.getId())) {
                user.setId(String.valueOf(snowflakeIdWorker.nextId()));
                user.setCreateTime(now);
            }
            user.setUpdateTime(now);
            Map<String, Object> params = ConfigModelUtil.convertUserInfoToMap(user);
            if (existing != null) {
                storageService.edit(StorageEnum.USER, params);
            } else {
                storageService.add(StorageEnum.USER, params);
            }
            if (firstId == null) {
                firstId = user.getId();
            }
        }
        for (UserInfo existing : existingByUsername.values()) {
            if (!keepUsernames.contains(existing.getUsername())) {
                storageService.remove(StorageEnum.USER, existing.getId());
            }
        }
        return firstId;
    }

    @Override
    public int countUsers() {
        Query query = new Query();
        query.setType(StorageEnum.USER);
        query.setQueryTotal(true);
        query.setPageNum(1);
        query.setPageSize(1);
        Paging paging = storageService.query(query);
        return paging == null ? 0 : (int) paging.getTotal();
    }

    @Override
    public boolean existsUser(String id) {
        if (StringUtil.isBlank(id)) {
            return false;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.USER);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, id);
        Paging paging = storageService.query(query);
        return paging != null && !CollectionUtils.isEmpty(paging.getData());
    }

    @Override
    public void removeUser(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.USER, id);
    }

    @Override
    public void importFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return;
        }
        List<UserConfig> configs = JsonUtil.jsonToArray(json, UserConfig.class);
        if (CollectionUtils.isEmpty(configs)) {
            return;
        }
        for (UserConfig config : configs) {
            syncUserConfig(config);
        }
    }
}
