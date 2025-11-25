package org.dbsyncer.web.integration;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * 测试配置类
 * 
 * 注意：
 * - 使用完整的 Web 环境（RANDOM_PORT），让 Spring Boot 自动配置所有 Actuator 端点
 * - 禁用 Spring Security 以避免影响测试
 * - 使用独立的配置类，在类上使用 @Order(0) 确保优先级高于 WebAppConfig（默认 Order 100）
 */
@TestConfiguration
@Profile("test")
public class TestActuatorConfiguration {

    /**
     * 测试专用的 Spring Security 配置
     * 禁用所有安全配置，允许所有请求通过
     * 
     * 注意：@Order 必须放在类上，不能放在 @Bean 方法上
     * 使用 @Order(0) 确保优先级高于 WebAppConfig（默认 Order 100）
     */
    @Bean
    public WebSecurityConfigurerAdapter testWebSecurityConfigurerAdapter() {
        return new TestWebSecurityConfig();
    }

    @Order(0)
    static class TestWebSecurityConfig extends WebSecurityConfigurerAdapter {
        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http
                .csrf().disable()
                .authorizeRequests()
                .anyRequest().permitAll()
                .and()
                .httpBasic().disable()
                .formLogin().disable();
        }
    }
}

