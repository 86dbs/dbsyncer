/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web;

import org.dbsyncer.common.util.DateFormatUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

@EnableAsync
@EnableScheduling
@SpringBootApplication(scanBasePackages = "org.dbsyncer")
public class Application {

    public static void main(String[] args) throws IOException {
        // 初始化日志目录，解决Log4j2启动时目录不存在的问题
        initLogDirectory();

        SpringApplication application = new SpringApplication(Application.class);
        setProperties(application);
        application.run(args);
    }

    /**
     * 创建日志目录（logs），确保目录存在
     */
    private static void initLogDirectory() {
        // 日志目录路径，与Log4j2配置中的路径保持一致
        File logDir = new File("logs");
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            if (created) {
                System.out.println("日志目录 logs 创建成功");
            } else {
                System.err.println("日志目录 logs 创建失败，请检查目录权限");
            }
        }
    }

    private static void setProperties(SpringApplication application) throws IOException {
        Resource location = new ClassPathResource("META-INF/build-info.properties");
        String version = "1.0.0-Release";
        Properties properties = new Properties();
        if (location.exists()) {
            BuildProperties build = new BuildProperties(loadFrom(location, "build"));
            version = build.getVersion();
            String buildTime = build.getTime().atZone(ZoneId.systemDefault()).format(DateFormatUtil.YYYY_MM_DD_HH_MM_SS);
            properties.put("info.app.build.time", buildTime);
        }
        properties.put("info.app.version", version);
        properties.put("info.app.current", Version.CURRENT);
        properties.put("info.app.start.time", LocalDateTime.now().format(DateFormatUtil.YYYY_MM_DD_HH_MM_SS));
        properties.put("spring.thymeleaf.prefix", "classpath:/public/");
        properties.put("management.endpoints.web.base-path", "/app");
        properties.put("management.endpoints.web.exposure.include", "*");
        properties.put("management.endpoint.health.show-details", "always");
        properties.put("management.health.elasticsearch.enabled", false);
        application.setDefaultProperties(properties);
    }

    private static Properties loadFrom(Resource location, String prefix) throws IOException {
        String p = (prefix.endsWith(".") ? prefix : prefix + ".");
        Properties source = PropertiesLoaderUtils.loadProperties(location);
        Properties target = new Properties();
        for (String key : source.stringPropertyNames()) {
            if (key.startsWith(p)) {
                target.put(key.substring(p.length()), source.get(key));
            }
        }
        return target;
    }
}