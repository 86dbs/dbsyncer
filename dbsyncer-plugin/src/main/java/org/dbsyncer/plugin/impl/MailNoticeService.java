package org.dbsyncer.plugin.impl;

import com.sun.mail.util.MailSSLSocketFactory;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.plugin.AbstractNoticeService;
import org.dbsyncer.plugin.model.HttpNoticeChannel;
import org.dbsyncer.plugin.model.NoticeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;

/**
 * 邮件通知服务实现
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/13 22:20
 */
public final class MailNoticeService extends AbstractNoticeService {

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

    public void build() throws GeneralSecurityException {
        final Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "smtp.qq.com");
        props.put("mail.user", username);
        props.put("mail.password", password);
        MailSSLSocketFactory sf = new MailSSLSocketFactory();
        sf.setTrustAllHosts(true);
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.ssl.socketFactory", sf);
        // 构建授权信息，用于进行SMTP身份验证
        session = Session.getInstance(props, new Authenticator() {

            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
    }

    @Override
    public void notify(NoticeMessage notice) {
        HttpNoticeChannel http = notice.getNoticeConfig().getHttp();
        if (!http.isEnabled()) {
            return;
        }
        try {
            checkMail(notice);
            // 统一应用标题
            AppConfig appConfig = getAppConfig();
            String title = String.format("【%s通知】%s", appConfig.getName(), notice.getTitle());
            String content = createTemplate(appConfig.getName(), notice.getContent());

            // 创建邮件消息
            MimeMessage message = new MimeMessage(session);
            // 设置发件人
            message.setFrom(new InternetAddress(username));

            // 接收人
            List<String> messageReceivers = notice.getReceivers();
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
        String temp = "<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "<meta charset=\"UTF-8\">\n" + "<title>${appName}通知</title>\n" + "</head>\n" + "<body>\n" + "${content}\n" + "<p><a href=\"http://gitee.com/ghi/dbsyncer\">访问项目</a></p>\n" + "</body>\n" + "</html>";
        String replace = StringUtil.replace(temp, "${appName}", appName);
        replace = StringUtil.replace(replace, "${content}", content);
        return replace;
    }

    private void checkMail(NoticeMessage notice) {
        Assert.notNull(notice, "通知请求不能为空");
        Assert.notNull(notice.getTitle(), "邮件主题不能为空");
        Assert.notNull(notice.getContent(), "邮件内容不能为空");
        Assert.notEmpty(notice.getReceivers(), "邮件收件人不能为空");
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}