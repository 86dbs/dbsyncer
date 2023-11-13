package org.dbsyncer.parser.message;

import org.dbsyncer.common.model.NotifyMessage;
import org.dbsyncer.common.spi.NotifyService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.UserConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class MessageServiceImpl implements MessageService {

    @Resource
    private NotifyService notifyService;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public void sendMessage(String title, String content) {
        UserConfig userConfig = profileComponent.getUserConfig();
        if (null == userConfig) {
            return;
        }

        List<String> mails = new ArrayList<>();
        userConfig.getUserInfoList().forEach(userInfo -> {
            if (StringUtil.isNotBlank(userInfo.getMail())) {
                Arrays.asList(StringUtil.split(userInfo.getMail(), ",")).forEach(mail -> mails.add(mail));
            }
        });
        if (CollectionUtils.isEmpty(mails)) {
            return;
        }
        notifyService.sendMessage(NotifyMessage.newBuilder().setTitle(title).setContent(content).setReceivers(mails));
    }

}