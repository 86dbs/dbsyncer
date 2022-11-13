package org.dbsyncer.plugin.notify;

import org.dbsyncer.common.model.NotifyMessage;
import org.dbsyncer.common.spi.NotifyService;
import org.dbsyncer.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.Properties;

/**
 * 邮件通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/13 22:20
 */
@Component
@ConditionalOnProperty(value = "dbsyncer.plugin.notify.email.enabled", havingValue = "true")
@ConfigurationProperties(prefix = "dbsyncer.plugin.notify.email")
public class EmailNotifyService implements NotifyService {

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
     * 邮件发送
     */
    private JavaMailSender mailSender;

    @PostConstruct
    private void init() {
        logger.info("username:{}", username);

        JavaMailSenderImpl sender = new JavaMailSenderImpl();
        //服务器
        sender.setHost("smtp.qq.com");
        //协议
        sender.setProtocol("smtps");
        //端口号
        sender.setPort(465);
        //邮箱账号
        sender.setUsername(username);
        //邮箱授权码
        sender.setPassword(password);
        //编码
        sender.setDefaultEncoding("Utf-8");
        Properties p = new Properties();
        p.setProperty("mail.smtp.ssl.enable", "true");
        sender.setJavaMailProperties(p);

        this.mailSender = sender;
    }

    @Override
    public void sendMessage(NotifyMessage notifyMessage) {
        checkMail(notifyMessage);

        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        // 发送人
        simpleMailMessage.setFrom(username);
        // 标题
        simpleMailMessage.setSubject(notifyMessage.getTitle());
        // 内容
        simpleMailMessage.setText(notifyMessage.getContent());
        // 接收人
        simpleMailMessage.setTo(StringUtil.split(notifyMessage.getReceiver(), ";"));
        simpleMailMessage.setSentDate(new Date());

        try {
            mailSender.send(simpleMailMessage);
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
}