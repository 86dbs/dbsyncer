package org.dbsyncer.biz.checker.impl.user;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class UserConfigChecker extends AbstractChecker {

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        UserConfig config = new UserConfig();
        config.setName("用户配置");
        config.setType(ConfigConstant.USER);
        config.setUserInfoList(new ArrayList<>());

        // 修改基本配置
        this.modifyConfigModel(config, params);
        return config;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        throw new BizException("Unsupported method");
    }

}