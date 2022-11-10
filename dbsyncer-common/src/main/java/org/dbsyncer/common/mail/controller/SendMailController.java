package org.dbsyncer.common.mail.controller;


import org.dbsyncer.common.mail.MailService;
import org.dbsyncer.common.mail.entity.MailRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/sendMail")
public class SendMailController {

    @Autowired
    private MailService mailService;

    @PostMapping(value = "/simple")
    public void sendSimpleMessage(@RequestBody MailRequest mailRequest){
        mailService.sendSimpleMail(mailRequest);
    }

}
