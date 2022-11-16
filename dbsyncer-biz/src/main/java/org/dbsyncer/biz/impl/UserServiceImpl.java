package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.UserService;
import org.dbsyncer.biz.vo.UserVo;
import org.springframework.stereotype.Service;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:16
 */
@Service
public class UserServiceImpl implements UserService {

    @Override
    public UserVo getUserVo(String username) {
        return new UserVo(username);
    }
}