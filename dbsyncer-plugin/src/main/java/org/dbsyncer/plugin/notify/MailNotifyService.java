package org.dbsyncer.plugin.notify;

import org.dbsyncer.common.model.NotifyMessage;
import org.dbsyncer.common.spi.NotifyService;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
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
            String to = notifyMessage.getReceiver();
            String title = notifyMessage.getTitle();
            String content = notifyMessage.getContent();

            // 创建邮件消息
            MimeMessage message = new MimeMessage(session);
            // 设置发件人
            message.setFrom(new InternetAddress(username));

            // 接收人
            String[] receiverArray = StringUtil.split(to, ";");
            int length = receiverArray.length;
            InternetAddress[] addresses = new InternetAddress[length];
            for (int i = 0; i < length; i++) {
                addresses[i] = new InternetAddress(receiverArray[i]);
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

    private void checkMail(NotifyMessage notifyMessage) {
        Assert.notNull(notifyMessage, "通知请求不能为空");
        Assert.notNull(notifyMessage.getTitle(), "邮件主题不能为空");
        Assert.notNull(notifyMessage.getContent(), "邮件内容不能为空");
        Assert.notNull(notifyMessage.getReceiver(), "邮件收件人不能为空");
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}