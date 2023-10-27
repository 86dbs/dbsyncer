package org.dbsyncer.common.config;

import org.dbsyncer.common.util.StringUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/18 0:04
 */
@Configuration
@ConfigurationProperties(prefix = "info.app")
public class AppConfig {

    private String name = "DBSyncer";

    private String version;

    private String copyright;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCopyright() {
        if (StringUtil.isBlank(copyright)) {
            StringBuilder copy = new StringBuilder();
            copy.append("&copy;").append(LocalDate.now().getYear()).append(" ");
            copy.append(name);
            copy.append("(").append(version).append(")");
            copy.append("<footer>Designed By <a href='https://gitee.com/ghi/dbsyncer' target='_blank' >AE86</a></footer>");
            this.copyright = copy.toString();
        }
        return copyright;
    }

    public void setCopyright(String copyright) {
        this.copyright = copyright;
    }

}