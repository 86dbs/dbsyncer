package org.dbsyncer.parser.message;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.NotifyMessage;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.spi.NotifyService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class MessageServiceImpl implements MessageService {

    private String userConfigId;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private NotifyService notifyService;

    @Autowired
    private StorageService storageService;

    @Autowired
    private Parser parser;

    @Override
    public void sendMessage(String title, String content) {
        UserConfig userConfig = getUserConfig();
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

    private UserConfig getUserConfig() {
        if (StringUtil.isNotBlank(userConfigId)) {
            return cacheService.get(userConfigId, UserConfig.class);
        }

        UserConfig userConfig = null;
        synchronized (this) {
            Query query = new Query();
            query.setType(StorageEnum.CONFIG);
            query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, ConfigConstant.USER);
            query.setPageNum(1);
            query.setPageSize(1);
            Paging paging = storageService.query(query);
            List<Map> data = (List<Map>) paging.getData();
            if (!CollectionUtils.isEmpty(data)) {
                Iterator iterator = data.iterator();
                if (iterator.hasNext()) {
                    Map map = (Map) iterator.next();
                    String json = (String) map.get(ConfigConstant.CONFIG_MODEL_JSON);
                    userConfig = parser.parseObject(json, UserConfig.class);
                    userConfigId = userConfig.getId();
                }

            }

        }
        return userConfig;
    }

}