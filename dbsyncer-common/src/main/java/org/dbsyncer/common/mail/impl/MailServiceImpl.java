package org.dbsyncer.common.mail.impl;

import org.dbsyncer.common.mail.MailService;
import org.dbsyncer.common.mail.entity.MailRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.Date;

@Service
public class MailServiceImpl implements MailService{

    private static final Logger LOG = LoggerFactory.getLogger(MailService.class);

    @Value("${spring.mail.username}")
    private String sender;

    // 注入邮件工具类
    @Autowired
    private JavaMailSender javaMailSender;

    public void checkMail(MailRequest mailRequest){
        Assert.notNull(mailRequest,"邮件请求不能为空");
        Assert.notNull(mailRequest.getReceiver(),"邮件收件人不能为空");
        Assert.notNull(mailRequest.getSubject(),"邮件主题不能为空");
        Assert.notNull(mailRequest.getText(),"邮件内容不能为空");
    }

    /**
     * 发送简单邮件
     *
     * @param mailRequest
     */
    @Override
    public void sendSimpleMail(MailRequest mailRequest){

        checkMail(mailRequest);

        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        simpleMailMessage.setFrom(sender);

        // 邮件发件人一个或多个
        String receiver = mailRequest.getReceiver();
        if (receiver.contains(";")){
            simpleMailMessage.setTo(receiver.split(";"));
        }else {
            simpleMailMessage.setTo(receiver);
        }

        simpleMailMessage.setSubject(mailRequest.getSubject());
        simpleMailMessage.setText(mailRequest.getText());
        simpleMailMessage.setSentDate(new Date());

        try {
            javaMailSender.send(simpleMailMessage);
            LOG.info("simple mail send success");
        }catch (Exception e){
            LOG.error("simpple mail send error!",e);
        }

    }

}
