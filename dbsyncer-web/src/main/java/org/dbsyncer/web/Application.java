package org.dbsyncer.web;

import org.dbsyncer.common.util.DateFormatUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.info.BuildProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;

@EnableAsync
@EnableScheduling
@EnableCaching
@SpringBootApplication(scanBasePackages = "org.dbsyncer", exclude = DataSourceAutoConfiguration.class)
public class Application {

    public static void main(String[] args) throws IOException {
        SpringApplication application = new SpringApplication(Application.class);
        setProperties(application);
        application.run(args);
    }

    private static void setProperties(SpringApplication application) throws IOException {
        Resource location = new ClassPathResource("META-INF/build-info.properties");
        String version = "1.0.0-Release";
        Properties properties = new Properties();
        if (location.exists()) {
            BuildProperties build = new BuildProperties(loadFrom(location, "build"));
            version = build.getVersion();
            long time = build.getTime().toEpochMilli();
            String format = DateFormatUtil.timestampToString(new Timestamp(time));
            properties.put("info.app.build.time", format);
        }
        properties.put("info.app.version", version);
        properties.put("spring.thymeleaf.prefix", "classpath:/public/");
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