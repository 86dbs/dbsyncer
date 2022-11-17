package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.UserService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.UserInfoVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private Manager manager;

    @Autowired
    private Checker userConfigChecker;

    @Override
    public synchronized String add(Map<String, String> params) {
        getUserConfig();
        ConfigModel configModel = userConfigChecker.checkAddConfigModel(params);
        return manager.addUserConfig(configModel);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel configModel = userConfigChecker.checkEditConfigModel(params);
        return manager.editUserConfig(configModel);
    }

    @Override
    public String getPassword(String username) {
        UserConfig userConfig = getUserConfig();
        return userConfig != null ? userConfig.getUserInfo(username).getPassword() : null;
    }

    @Override
    public UserInfoVo getUserInfoVo(String username) {
        return convertUserConfig2Vo(getUserConfig().getUserInfo(username));
    }

    private synchronized UserConfig getUserConfig() {
        List<UserConfig> all = manager.getUserConfigAll();
        return CollectionUtils.isEmpty(all) ? (UserConfig) userConfigChecker.checkAddConfigModel(new HashMap<>()) : all.get(0);
    }

    private UserInfoVo convertUserConfig2Vo(UserInfo userInfo) {
        UserInfoVo userInfoVo = new UserInfoVo();
        BeanUtils.copyProperties(userInfo, userInfoVo);
        // 避免密码直接暴露
        userInfoVo.setPassword("");
        return userInfoVo;
    }

}