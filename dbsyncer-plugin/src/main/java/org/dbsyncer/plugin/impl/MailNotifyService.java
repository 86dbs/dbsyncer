package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.NotifyMessage;
import org.dbsyncer.sdk.spi.NotifyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

/**
 * 邮件通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/13 22:20
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.plugin.notify.mail.enabled", havingValue = "true")
@ConfigurationProperties(prefix = "dbsyncer.plugin.notify.mail")
public class MailNotifyService implements NotifyService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private AppConfig appConfig;

    /**
     * 邮箱账户
     */
    private String username;

    /**
     * 授权码
     */
    private String password;

    /**
     * 邮箱会话
     */
    private Session session;

    @PostConstruct
    private void init() {
        final Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "smtp.qq.com");
        props.put("mail.user", username);
        props.put("mail.password", password);

        // 构建授权信息，用于进行SMTP进行身份验证
        session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
    }

    @Override
    public void sendMessage(NotifyMessage notifyMessage) {
        try {
            checkMail(notifyMessage);
            // 统一应用标题
            String title = String.format("【%s通知】%s", appConfig.getName(), notifyMessage.getTitle());
            String content = createTemplate(appConfig.getName(), notifyMessage.getContent());

            // 创建邮件消息
            MimeMessage message = new MimeMessage(session);
            // 设置发件人
            message.setFrom(new InternetAddress(username));

            // 接收人
            List<String> messageReceivers = notifyMessage.getReceivers();
            int size = messageReceivers.size();
            InternetAddress[] addresses = new InternetAddress[size];
            for (int i = 0; i < size; i++) {
                addresses[i] = new InternetAddress(messageReceivers.get(i));
            }
            message.setRecipients(Message.RecipientType.TO, addresses);

            // 设置邮件标题
            message.setSubject(title);

            // 设置邮件的内容体
            message.setContent(content, "text/html;charset=UTF-8");
            // 发送邮件
            Transport.send(message);
            logger.info("simple mail send success");
        } catch (Exception e) {
            logger.error("simple mail send error!", e);
        }
    }

    private String createTemplate(String appName, String content) {
        String temp = "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<meta charset=\"UTF-8\">\n" +
                "<title>${appName}通知</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "${content}\n" +
                "<p><a href=\"http://gitee.com/ghi/dbsyncer\">访问项目</a></p>\n" +
                "</body>\n" +
                "</html>";
        String replace = StringUtil.replace(temp, "${appName}", appName);
        replace = StringUtil.replace(replace, "${content}", content);
        return replace;
    }

    private void checkMail(NotifyMessage notifyMessage) {
        Assert.notNull(notifyMessage, "通知请求不能为空");
        Assert.notNull(notifyMessage.getTitle(), "邮件主题不能为空");
        Assert.notNull(notifyMessage.getContent(), "邮件内容不能为空");
        Assert.notEmpty(notifyMessage.getReceivers(), "邮件收件人不能为空");
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}