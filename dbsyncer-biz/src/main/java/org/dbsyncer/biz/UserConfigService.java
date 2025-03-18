/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.UserInfoVo;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;

import java.util.List;
import java.util.Map;

/**
 * 用戶配置服務
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
public interface UserConfigService {

    /**
     * 获取登录用户属性KEY
     */
    String CURRENT_USER_NAME = "currentUserName";

    /**
     * 新增用戶(仅管理员可以新增用户)
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 修改用戶(管理员可以修改所有用户，普通用户只能修改自己)
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 删除用戶(仅管理员可以删除普通用户)
     *
     * @param params
     */
    String remove(Map<String, String> params);

    /**
     * 获取登录用户密码
     *
     * @param currentUserName 登录用户
     * @return
     */
    UserInfo getUserInfo(String currentUserName);

    /**
     * 获取登录用户信息VO(管理员可以查看所有用户，普通用户只能查看自己)
     *
     * @param currentUserName 登录用户
     * @param username        查询的用户
     * @return
     */
    UserInfoVo getUserInfoVo(String currentUserName, String username);

    /**
     * 获取所有用户信息VO(系统管理员可以查看所有用户，其他用户只能查看自己)
     *
     * @param currentUserName 登录用户
     * @return
     */
    List<UserInfoVo> getUserInfoAll(String currentUserName);

    /**
     * 获取用户配置
     *
     * @return
     */
    UserConfig getUserConfig();

    /**
     * 获取默认用户(admin)
     *
     * @return
     */
    UserInfo getDefaultUser();

}