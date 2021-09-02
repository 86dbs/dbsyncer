package org.dbsyncer.web.config;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.SHA1Util;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/23 23:57
 */
@Configuration
@EnableWebSecurity
public class WebAppConfig extends WebSecurityConfigurerAdapter implements AuthenticationProvider, HttpSessionListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 认证地址
     */
    private static final String LOGIN = "/login";

    /**
     * 认证页面
     */
    private static final String LOGIN_PAGE = "/login.html";

    /**
     * 每个帐号允许同时登录会话数, 默认同一个帐号只能在一个地方登录
     */
    private static final int MAXIMUM_SESSIONS = 1;

    @Value(value = "${dbsyncer.web.login.username}")
    private String username;

    @Autowired
    private ConfigService configService;

    /**
     * 登录失败
     *
     * @return
     */
    @Bean
    public AuthenticationFailureHandler loginFailHandler() {
        return (request, response, e) -> write(response, RestResult.restFail(e.getMessage(), 401));
    }

    /**
     * 登录成功
     *
     * @return
     */
    @Bean
    public SavedRequestAwareAuthenticationSuccessHandler loginSuccessHandler() {
        return new SavedRequestAwareAuthenticationSuccessHandler() {
            @Override
            public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) {
                Object principal = authentication.getPrincipal();
                logger.info("USER : " + principal + " LOGIN SUCCESS !  ");
                write(response, RestResult.restSuccess("登录成功!"));
            }
        };
    }

    @Bean
    public LogoutSuccessHandler logoutHandler() {
        return (request, response, authentication) -> {
            try {
                Object principal = authentication.getPrincipal();
                logger.info("USER : {} LOGOUT SUCCESS ! ", principal);
                write(response, RestResult.restSuccess("注销成功!"));
            } catch (Exception e) {
                logger.info("LOGOUT EXCEPTION , e : {}", e.getMessage());
                write(response, RestResult.restFail(e.getMessage(), 403));
            }
        };
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        //http.csrf().disable()
        //        .authorizeRequests()
        //        .anyRequest().permitAll()
        //        .and().logout().permitAll();

        http.csrf().disable()
                .authorizeRequests()
                .antMatchers("/css/**", "/js/**", "/img/**", "/config/**", "/plugins/**", "/index/version.json").permitAll().anyRequest()
                .authenticated()
                .and()
                .formLogin()
                .loginProcessingUrl(LOGIN)
                .loginPage(LOGIN_PAGE)
                .successHandler(loginSuccessHandler())
                .failureHandler(loginFailHandler())
                .permitAll()
                .and()
                .logout()
                .permitAll()
                .invalidateHttpSession(true).deleteCookies("JSESSIONID").logoutSuccessHandler(logoutHandler())
                .and()
                .sessionManagement()
                .sessionFixation()
                .migrateSession()
                .maximumSessions(MAXIMUM_SESSIONS);
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        // 获取表单用户名
        String username = (String) authentication.getPrincipal();
        // 获取表单用户填写的密码
        String password = (String) authentication.getCredentials();
        password = SHA1Util.b64_sha1(password);

        if (!StringUtil.equals(username, this.username) || !StringUtil.equals(configService.getPassword(), password)) {
            throw new BadCredentialsException("对不起,您输入的帐号或密码错误");
        }
        return new UsernamePasswordAuthenticationToken(username, password, AuthorityUtils.commaSeparatedStringToAuthorityList("admin"));
    }

    @Override
    public boolean supports(Class<?> aClass) {
        return true;
    }

    @Override
    public void sessionCreated(HttpSessionEvent se) {
        logger.debug("创建会话:{}", se.getSession().getId());
        int maxInactiveInterval = se.getSession().getMaxInactiveInterval();
        logger.debug(String.valueOf(maxInactiveInterval));
    }

    @Override
    public void sessionDestroyed(HttpSessionEvent se) {
        logger.debug("销毁会话:{}", se.getSession().getId());
    }

    /**
     * 响应
     *
     * @param response
     * @param result
     */
    private void write(HttpServletResponse response, RestResult result) {
        PrintWriter out = null;
        try {
            response.setContentType("application/json;charset=utf-8");
            response.setStatus(result.getStatus());
            out = response.getWriter();
            out.write(JsonUtil.objToJson(result));
            out.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if (null != out) {
                out.close();
            }
        }
    }

}