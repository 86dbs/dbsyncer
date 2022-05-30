package org.dbsyncer.web.config;

import org.dbsyncer.web.remote.UserService;
import org.dbsyncer.web.remote.UserServiceImpl;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;
import org.springframework.util.unit.DataSize;

import javax.servlet.MultipartConfigElement;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/10 23:07
 */
@Configuration
public class OpenApiConfig {

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

    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        //上传的单个文件最大值   KB,MB 这里设置为10MB
        DataSize maxSize = DataSize.ofMegabytes(10);
        DataSize requestMaxSize = DataSize.ofMegabytes(30);
        factory.setMaxFileSize(maxSize);
        /// 设置一次上传文件的总大小
        factory.setMaxRequestSize(requestMaxSize);
        return factory.createMultipartConfig();
    }

}