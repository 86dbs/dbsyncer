/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.config;

import org.dbsyncer.web.controller.openapi.OpenApiInterceptor;

import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;
import javax.servlet.MultipartConfigElement;

/**
 * Web配置
 * 
 * @author 穿云
 * @version 1.0.0
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Resource
    private OpenApiInterceptor openApiInterceptor;

    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        // 上传单个文件最大值MB
        DataSize maxSize = DataSize.ofMegabytes(128);
        DataSize requestMaxSize = DataSize.ofMegabytes(128);
        factory.setMaxFileSize(maxSize);
        // 设置一次上传文件的总大小
        factory.setMaxRequestSize(requestMaxSize);
        return factory.createMultipartConfig();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 注册OpenAPI拦截器，拦截/openapi/**路径
        registry.addInterceptor(openApiInterceptor).addPathPatterns("/openapi/**");
    }
}