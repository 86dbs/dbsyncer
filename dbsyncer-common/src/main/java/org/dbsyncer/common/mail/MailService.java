package org.dbsyncer.common.mail;

import org.dbsyncer.common.mail.entity.MailRequest;

public interface MailService {

    void sendSimpleMail(MailRequest mailRequest);

}
