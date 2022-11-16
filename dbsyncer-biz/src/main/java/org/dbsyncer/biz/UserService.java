package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.UserVo;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
public interface UserService {

    /**
     * 获取登录用户信息
     *
     * @param username
     * @return
     */
    UserVo getUserVo(String username);

}