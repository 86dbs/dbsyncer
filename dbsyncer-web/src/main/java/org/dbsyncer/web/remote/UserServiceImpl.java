package org.dbsyncer.web.remote;

import java.time.LocalDateTime;

public class UserServiceImpl implements UserService {

    private String name = "def";

    @Override
    public String hello(String params) {
        return name + LocalDateTime.now().toString();
    }

}