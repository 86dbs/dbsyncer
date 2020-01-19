package org.dbsyncer.web.config;

import org.dbsyncer.web.remote.UserService;
import org.dbsyncer.web.remote.UserServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;

/**
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:07
 */
@Configuration
public class AppConfig {

    /**
     * 发布服务接口
     * @return
     */
    @Bean("/remoteService")
    public HttpInvokerServiceExporter remoteService() {
        HttpInvokerServiceExporter httpInvokerServiceExporter = new HttpInvokerServiceExporter();
        httpInvokerServiceExporter.setService(new UserServiceImpl());
        httpInvokerServiceExporter.setServiceInterface(UserService.class);
        return httpInvokerServiceExporter;
    }

    /**
     * 客户端，代理工厂HttpInvokerProxyFactoryBean，用于创建HttpInvoker代理来与远程服务通信
     *
     * @return
     */
    @Bean
    public HttpInvokerProxyFactoryBean userService() {
        HttpInvokerProxyFactoryBean proxy = new HttpInvokerProxyFactoryBean();
        proxy.setServiceUrl("http://localhost:18686/userService");
        proxy.setServiceInterface(UserService.class);
        return proxy;
    }

}
