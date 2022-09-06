package org.dbsyncer.web.config;

import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;

import javax.servlet.MultipartConfigElement;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/6 23:07
 */
@Configuration
public class WebConfig {

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
}