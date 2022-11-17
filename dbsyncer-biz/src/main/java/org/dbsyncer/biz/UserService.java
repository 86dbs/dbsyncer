package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.UserInfoVo;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
public interface UserService {

    /**
     * 新增用戶
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 修改用戶
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 获取用户密码
     *
     * @return
     */
    String getPassword(String username);

    /**
     * 获取用户信息VO
     *
     * @param username
     * @return
     */
    UserInfoVo getUserInfoVo(String username);

}