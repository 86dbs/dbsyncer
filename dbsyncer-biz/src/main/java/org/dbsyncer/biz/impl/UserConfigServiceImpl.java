/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PermissionChecker;
import org.dbsyncer.biz.PermissionService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.checker.impl.user.UserConfigChecker;
import org.dbsyncer.biz.enums.UserRoleEnum;
import org.dbsyncer.biz.vo.UserInfoVO;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
@Service
public class UserConfigServiceImpl implements UserConfigService {

    private static final String DEFAULT_USERNAME = "admin";

    private static final String DEFAULT_PASSWORD = "0DPiKuNIrrVmD8IUCuw1hQxNqZc=";

    private static final String PERM_USER_EDIT = "user:edit";

    @Resource
    private UserProfile userProfile;

    @Resource
    private UserConfigChecker userConfigChecker;

    @Resource
    private LogService logService;

    @Resource
    private PermissionService permissionService;

    @Resource
    private PermissionChecker permissionChecker;

    @Override
    public synchronized String add(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");
        String nickname = params.get("nickname");
        Assert.hasText(nickname, "The nickname is null.");
        String password = params.get("password");
        Assert.hasText(password, "The password is null.");
        String email = params.get("email");
        String phone = params.get("phone");

        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_USER_EDIT);

        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(currentUserName);
        Assert.notNull(currentUser, "无法获取登录用户.");
        Assert.isNull(userConfig.getUserInfo(username), "用户已存在，请换个账号");

        userConfig.getUserInfoList().add(new UserInfo(username, nickname, SHA1Util.b64_sha1(password),
                UserRoleEnum.USER.getCode(), email, phone));

        logService.log(LogType.UserLog.INSERT, String.format("[%s]添加[%s]账号成功", currentUser.getUsername(), username));
        String result = userProfile.saveUserConfig(userConfig);
        UserInfo created = getUserConfig().getUserInfo(username);
        if (created != null && StringUtil.isNotBlank(created.getId())) {
            permissionService.bindUserRole(created.getId(), UserRoleEnum.USER.getCode());
        }
        return result;
    }

    @Override
    public synchronized String edit(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");
        String nickname = params.get("nickname");
        Assert.hasText(nickname, "The nickname is null.");
        String newPwd = params.get("newPwd");
        String email = params.get("email");
        String phone = params.get("phone");

        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(currentUserName);
        Assert.notNull(currentUser, "无法获取登录用户.");
        boolean canEditOthers = permissionService.hasPermission(currentUserName, PERM_USER_EDIT);
        boolean self = StringUtil.equals(currentUser.getUsername(), username);
        Assert.isTrue(canEditOthers || self, "No permission.");

        UserInfo updateUser = self ? currentUser : userConfig.getUserInfo(username);
        Assert.notNull(updateUser, "用户不存在");

        updateUser.setNickname(nickname);
        updateUser.setEmail(email);
        updateUser.setPhone(phone);
        if (StringUtil.isNotBlank(newPwd)) {
            if (self) {
                String oldPwd = params.get("oldPwd");
                Assert.hasText(oldPwd, "旧密码不能为空.");
                if (!StringUtil.equals(SHA1Util.b64_sha1(oldPwd), updateUser.getPassword())) {
                    logService.log(LogType.SystemLog.ERROR, String.format("[%s]修改密码失败", username));
                    throw new BizException("修改密码失败.");
                }
            }
            newPwd = SHA1Util.b64_sha1(newPwd);
            Assert.isTrue(!StringUtil.equals(newPwd, updateUser.getPassword()), "新旧密码不能完全一样.");
            updateUser.setPassword(newPwd);
            logService.log(LogType.UserLog.UPDATE, String.format("[%s]修改[%s]账号密码成功", currentUser.getUsername(), username));
        }

        return userProfile.saveUserConfig(userConfig);
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");

        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_USER_EDIT);

        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(currentUserName);
        Assert.notNull(currentUser, "无法获取登录用户.");
        Assert.isTrue(!StringUtil.equals(currentUser.getUsername(), username), "不能删除自己.");

        UserInfo deleteUser = userConfig.getUserInfo(username);
        Assert.notNull(deleteUser, "用户已删除.");
        if (StringUtil.isNotBlank(deleteUser.getId())) {
            permissionService.unbindAllUserRoles(deleteUser.getId());
        }
        userConfig.removeUserInfo(username);
        userProfile.saveUserConfig(userConfig);
        logService.log(LogType.UserLog.DELETE, String.format("[%s]删除[%s]账号成功", currentUser.getUsername(), username));
        return "删除用户成功!";
    }

    @Override
    public UserInfo getUserInfo(String currentUserName) {
        return getUserConfig().getUserInfo(currentUserName);
    }

    @Override
    public UserInfoVO getUserInfoVo(String currentUserName, String username) {
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(currentUserName);
        Assert.notNull(currentUser, "无法获取登录用户.");
        boolean admin = permissionService.isAdmin(currentUserName);
        boolean self = StringUtil.equals(currentUser.getUsername(), username);
        Assert.isTrue(admin || self, "No permission.");

        UserInfo userInfo = getUserConfig().getUserInfo(username);
        return convertUserInfo2Vo(userInfo);
    }

    @Override
    public List<UserInfoVO> getUserInfoAll(String currentUserName) {
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(currentUserName);
        Assert.notNull(currentUser, "无法获取登录用户.");
        if (permissionService.isAdmin(currentUserName)) {
            return getUserConfig().getUserInfoList().stream().map(this::convertUserInfo2Vo).collect(Collectors.toList());
        }

        List<UserInfoVO> list = new ArrayList<>();
        list.add(convertUserInfo2Vo(currentUser));
        return list;
    }

    @Override
    public UserConfig getUserConfig() {
        UserConfig config = userProfile.getUserConfig();
        if (null != config) {
            return config;
        }

        synchronized (this) {
            config = userProfile.getUserConfig();
            if (null == config) {
                config = (UserConfig) userConfigChecker.checkAddConfigModel(new HashMap<>());
                config.getUserInfoList().add(getDefaultUser());
                userProfile.saveUserConfig(config);
                UserInfo admin = getUserConfig().getUserInfo(DEFAULT_USERNAME);
                if (admin != null && StringUtil.isNotBlank(admin.getId())) {
                    permissionService.bindUserRole(admin.getId(), UserRoleEnum.ADMIN.getCode());
                }
            }
            return config;
        }
    }

    @Override
    public UserInfo getDefaultUser() {
        return new UserInfo(DEFAULT_USERNAME, DEFAULT_USERNAME, DEFAULT_PASSWORD, UserRoleEnum.ADMIN.getCode(), StringUtil.EMPTY, StringUtil.EMPTY);
    }

    private UserInfoVO convertUserInfo2Vo(UserInfo userInfo) {
        UserInfoVO userInfoVo = new UserInfoVO();
        if (null != userInfo) {
            BeanUtils.copyProperties(userInfo, userInfoVo);
            userInfoVo.setPassword("***");
            userInfoVo.setRoleName(UserRoleEnum.getNameByCode(userInfo.getRoleCode()));
        }
        return userInfoVo;
    }

}
