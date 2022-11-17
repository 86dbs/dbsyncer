package org.dbsyncer.biz.checker.impl.user;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class UserConfigChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value(value = "${dbsyncer.web.login.username}")
    private String username;

    @Value(value = "${dbsyncer.web.login.password}")
    private String password;

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Override
    public synchronized ConfigModel checkAddConfigModel(Map<String, String> params) {
        UserConfig config = new UserConfig();
        config.setName("用户配置");
        config.setType(ConfigConstant.USER_CONFIG);
        List<UserInfo> list = new ArrayList<>();
        list.add(new UserInfo(username, "系统管理员", password));
        config.setUserInfoList(list);

        // 修改基本配置
        this.modifyConfigModel(config, params);

        manager.addUserConfig(config);
        return config;
    }

    @Override
    public synchronized ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "UserConfig check params is null.");

        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Assert.hasText(id, "UserConfig id is empty.");
        UserConfig config = manager.getUserConfig(id);
        Assert.notNull(config, "用户配置文件为空.");
        String username = params.get("username");
        Assert.hasText(username, "UserConfig username is empty.");
        UserInfo userInfo = config.getUserInfo(username);
        Assert.notNull(userInfo, "无效的用户.");

        String nickname = params.get("nickname");
        Assert.hasText(nickname, "UserConfig nickname is empty.");
        // 修改密码
        String newPwd = params.get("newPwd");
        String oldPwd = params.get("oldPwd");
        if (StringUtil.isNotBlank(newPwd) && StringUtil.isNotBlank(oldPwd)) {
            oldPwd = SHA1Util.b64_sha1(oldPwd);

            if (!StringUtil.equals(userInfo.getPassword(), oldPwd)) {
                logService.log(LogType.SystemLog.ERROR, "修改密码失败");
                throw new BizException("修改密码失败");
            }
            userInfo.setPassword(SHA1Util.b64_sha1(newPwd));
            logService.log(LogType.SystemLog.INFO, "修改密码成功");
        }

        userInfo.setNickname(nickname);
        userInfo.setPassword(password);
        logService.log(LogType.SystemLog.INFO, String.format("[%s]修改密码成功", username));

        // 修改基本配置
        this.modifyConfigModel(config, params);
        return config;
    }

}