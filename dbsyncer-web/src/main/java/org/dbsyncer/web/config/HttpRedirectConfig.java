/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.config;

import org.apache.catalina.connector.Connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2025/10/23 18:30
 */
@Configuration
@ConditionalOnProperty(name = {"server.ssl.enabled", "server.http.enabled"}, havingValue = "true")
public class HttpRedirectConfig implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${server.port:18686}")
    private int httpsPort;

    @Value("${server.http.port:8080}")
    private int httpPort;

    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        logger.info("Configuring HTTP on port: {}", httpPort);
        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        connector.setScheme("http");
        connector.setPort(httpPort);
        connector.setSecure(false);
        connector.setRedirectPort(httpsPort);
        factory.addAdditionalTomcatConnectors(connector);
    }

    // 配置过滤器顺序，确保在安全过滤器之前执行
    @Bean
    @ConditionalOnProperty(value = "server.http.redirect", havingValue = "true")
    public FilterRegistrationBean<Filter> httpsRedirectFilter() {
        FilterRegistrationBean<Filter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new HttpsRedirectFilter());
        registrationBean.addUrlPatterns("/*");
        registrationBean.setOrder(1); // 高优先级
        return registrationBean;
    }

    // 重定向过滤器实现
    public class HttpsRedirectFilter implements Filter {

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            // 如果是HTTP请求且端口是HTTP端口，则重定向到HTTPS
            if (!httpRequest.isSecure() && httpRequest.getLocalPort() == httpPort) {
                StringBuffer url = httpRequest.getRequestURL();
                String queryString = httpRequest.getQueryString();

                // 构建HTTPS URL
                String httpsUrl = url.toString().replaceFirst("http://", "https://").replace(":" + httpPort, ":" + httpsPort);

                if (queryString != null) {
                    httpsUrl += "?" + queryString;
                }

                httpResponse.sendRedirect(httpsUrl);
                return;
            }

            chain.doFilter(request, response);
        }
    }
}