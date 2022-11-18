package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.UserService;
import org.dbsyncer.biz.checker.impl.user.UserConfigChecker;
import org.dbsyncer.biz.enums.UserEnum;
import org.dbsyncer.biz.vo.UserInfoVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

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
public class UserServiceImpl implements UserService {

    @Value(value = "${dbsyncer.web.login.username}")
    private String username;

    @Value(value = "${dbsyncer.web.login.password}")
    private String password;

    @Autowired
    private Manager manager;

    @Autowired
    private UserConfigChecker userConfigChecker;

    @Autowired
    private LogService logService;

    @Override
    public synchronized String add(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");
        String nickname = params.get("nickname");
        Assert.hasText(nickname, "The nickname is null.");
        String password = params.get("password");
        Assert.hasText(password, "The password is null.");

        // 验证当前登录用户合法身份（必须是管理员操作）
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(params.get(UserService.CURRENT_USER_NAME));
        Assert.isTrue(null == currentUser || UserEnum.isAdmin(currentUser.getRoleCode()), "No permission.");
        // 新用户合法性（用户不能重复）
        Assert.isNull(userConfig.getUserInfo(username), "用户已存在");
        // 注册新用户
        userConfig.getUserInfoList().add(new UserInfo(username, nickname, SHA1Util.b64_sha1(password), UserEnum.USER.getCode()));

        return manager.editUserConfig(userConfig);
    }

    @Override
    public synchronized String edit(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");
        String nickname = params.get("nickname");
        Assert.hasText(nickname, "The nickname is null.");
        String newPwd = params.get("newPwd");

        // 验证当前登录用户合法身份（管理员或本人操作）
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(params.get(UserService.CURRENT_USER_NAME));
        boolean admin = null != currentUser && UserEnum.isAdmin(currentUser.getRoleCode());
        boolean self = null != currentUser && StringUtil.equals(currentUser.getUsername(), username);
        Assert.isTrue(admin || self, "No permission.");

        // 修改自己或其他用户信息
        UserInfo updateUser = self ? currentUser : userConfig.getUserInfo(username);
        Assert.notNull(updateUser, "用户不存在");

        // 用户昵称
        updateUser.setNickname(nickname);
        // 修改密码
        if(StringUtil.isNotBlank(newPwd)){
            // 修改自己的密码需要验证
            if(self){
                String oldPwd = params.get("oldPwd");
                Assert.hasText(oldPwd, "The oldPwd is null.");
                if(!StringUtil.equals(SHA1Util.b64_sha1(oldPwd), updateUser.getPassword())){
                    logService.log(LogType.SystemLog.ERROR, String.format("[%s]修改密码失败", username));
                    throw new BizException("修改密码失败");
                }
            }
            newPwd = SHA1Util.b64_sha1(newPwd);
            Assert.isTrue(!StringUtil.equals(newPwd, updateUser.getPassword()), "新旧密码不能完全一样.");
            updateUser.setPassword(newPwd);
            logService.log(LogType.SystemLog.INFO, String.format("[%s]修改账号[%s]密码成功", currentUser.getUsername(), username));
        }

        return manager.editUserConfig(userConfig);
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        String username = params.get("username");
        Assert.hasText(username, "The username is null.");

        // 验证当前登录用户合法身份（必须是管理员操作）
        UserConfig userConfig = getUserConfig();
        UserInfo currentUser = userConfig.getUserInfo(params.get(UserService.CURRENT_USER_NAME));
        Assert.isTrue(UserEnum.isAdmin(currentUser.getRoleCode()), "No permission.");

        // 删除用户
        UserInfo deleteUser = userConfig.getUserInfo(username);
        Assert.notNull(deleteUser, "用户已删除.");
        userConfig.removeUserInfo(username);
        manager.editUserConfig(userConfig);
        return "删除用户成功!";
    }

    @Override
    public UserInfo getUserInfo(String currentUserName) {
        return getUserConfig().getUserInfo(currentUserName);
    }

    @Override
    public UserInfoVo getUserInfoVo(String currentUserName) {
        return convertUserInfo2Vo(getUserConfig().getUserInfo(currentUserName));
    }

    @Override
    public List<UserInfoVo> getUserInfoAll(String currentUserName) {
        return getUserConfig().getUserInfoList().stream().map(user -> convertUserInfo2Vo(user)).collect(Collectors.toList());
    }

    private UserConfig getUserConfig() {
        List<UserConfig> all = manager.getUserConfigAll();
        if (!CollectionUtils.isEmpty(all)) {
            return all.get(0);
        }

        synchronized (this) {
            all = manager.getUserConfigAll();
            if (!CollectionUtils.isEmpty(all)) {
                return all.get(0);
            }

            UserConfig userConfig = (UserConfig) userConfigChecker.checkAddConfigModel(new HashMap<>());
            UserEnum admin = UserEnum.ADMIN;
            userConfig.getUserInfoList().add(new UserInfo(username, admin.getName(), password, admin.getCode()));
            manager.addUserConfig(userConfig);
            return userConfig;
        }
    }

    private UserInfoVo convertUserInfo2Vo(UserInfo userInfo) {
        UserInfoVo userInfoVo = new UserInfoVo();
        BeanUtils.copyProperties(userInfo, userInfoVo);
        // 避免密码直接暴露
        userInfoVo.setPassword("");
        return userInfoVo;
    }

}