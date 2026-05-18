package org.dbsyncer.plugin.impl;

import com.sun.mail.util.MailSSLSocketFactory;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.model.MailNoticeChannel;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.plugin.model.ConnectorOfflineContent;
import org.dbsyncer.plugin.model.MappingErrorContent;
import org.dbsyncer.plugin.model.MappingStopContent;
import org.dbsyncer.plugin.model.TestNoticeContent;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeMessage;
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
        MailNoticeChannel mail = notice.getNoticeConfig().getMail();
        if (!mail.isEnabled()) {
            return;
        }
        try {
            checkMail(notice);
            // 统一应用标题
            AppConfig appConfig = getAppConfig();
            NoticeContent noticeContent = notice.getNoticeContent();
            String title = String.format("[%s] %s", appConfig.getName(), noticeContent.getTitle());
            String content = createTemplate(appConfig.getName(), buildContent(noticeContent));
            if (StringUtil.isBlank(content)) {
                logger.warn("simple mail send content is blank, skip send, notice content:{}", noticeContent.getClass().getName());
                return;
            }

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

    private String buildContent(NoticeContent noticeContent) {
        // 运行异常
        if (noticeContent instanceof MappingErrorContent) {
            return getMappingErrorContent((MappingErrorContent) noticeContent);
        }

        // 停止驱动
        if (noticeContent instanceof MappingStopContent) {
            MappingStopContent meta = (MappingStopContent) noticeContent;
            return String.format("驱动：%s(%s)", meta.getName(), meta.getModel().getName());
        }

        // 连接离线
        if (noticeContent instanceof ConnectorOfflineContent) {
            return getConnectorOfflineContent((ConnectorOfflineContent) noticeContent);
        }

        // 测试通知
        if (noticeContent instanceof TestNoticeContent) {
            TestNoticeContent meta = (TestNoticeContent) noticeContent;
            return meta.getContent();
        }
        return null;
    }

    private String getConnectorOfflineContent(ConnectorOfflineContent noticeContent) {
        StringBuilder c = new StringBuilder();
        for (int i = 0; i < noticeContent.getErrorItems().size(); i++) {
            ConnectorOfflineContent.ErrorItem item = noticeContent.getErrorItems().get(i);
            c.append("<p>");
            c.append(String.format("%d. %s(%s), %s", i+1, item.getName(), item.getType(), item.getUrl()));
            c.append("<p>");
        }
        return c.toString();
    }

    private static String getMappingErrorContent(MappingErrorContent noticeContent) {
        StringBuilder c = new StringBuilder();
        for (int i = 0; i < noticeContent.getErrorItems().size(); i++) {
            MappingErrorContent.ErrorItem item = noticeContent.getErrorItems().get(i);
            c.append("<p>");
            c.append(String.format("%d. %s(%s) 失败:%s, 成功:%s", i+1, item.getName(), item.getModel().getName(), item.getFail(), item.getSuccess()));
            if (ModelEnum.FULL == item.getModel()) {
                c.append(String.format(", 总数:%s", item.getTotal()));
            }
            c.append("<p>");
        }
        return c.toString();
    }

    private String createTemplate(String appName, String content) {
        String temp = "<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "<meta charset=\"UTF-8\">\n" + "<title>${appName}通知</title>\n" + "</head>\n" + "<body>\n" + "${content}\n" + "<p><a href=\"http://gitee.com/ghi/dbsyncer\">访问项目</a></p>\n" + "</body>\n" + "</html>";
        String replace = StringUtil.replace(temp, "${appName}", appName);
        replace = StringUtil.replace(replace, "${content}", content);
        return replace;
    }

    private void checkMail(NoticeMessage notice) {
        Assert.notNull(notice, "通知请求不能为空");
        Assert.notNull(notice.getNoticeContent(), "通知请求信息不能为空");
        Assert.notNull(notice.getNoticeContent().getTitle(), "邮件主题不能为空");
        Assert.notEmpty(notice.getReceivers(), "邮件收件人不能为空");
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}