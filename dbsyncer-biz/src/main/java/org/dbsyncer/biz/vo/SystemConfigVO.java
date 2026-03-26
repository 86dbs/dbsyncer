package org.dbsyncer.biz.vo;

import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.model.SecretVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.SystemConfig;

import java.util.List;

public class SystemConfigVO extends SystemConfig {

    /**
     * IP 白名单列表转为换行分隔的字符串，供前端 textarea 展示
     */
    public String getIpWhitelistText() {
        if (getIpWhitelistConfig() == null || getIpWhitelistConfig().getWhitelist() == null) {
            return "";
        }
        List<String> list = getIpWhitelistConfig().getWhitelist();
        return CollectionUtils.isEmpty(list) ? "" : String.join("\n", list);
    }

    /**
     * 获取最新的API 密钥
     */
    public RsaVersion getCurrentRsa() {
        if (getRsaConfig() != null) {
            List<RsaVersion> versions = getRsaConfig().getRsaVersions();
            if (!CollectionUtils.isEmpty(versions)) {
                return versions.get(versions.size() - 1);
            }
        }
        return null;
    }

    /**
     * 获取最新的API 密钥
     */
    public String getCurrentSecret() {
        if (getApiKeyConfig() != null) {
            List<SecretVersion> versions = getApiKeyConfig().getSecrets();
            if (!CollectionUtils.isEmpty(versions)) {
                return versions.get(versions.size() - 1).getSecret();
            }
        }
        return StringUtil.EMPTY;
    }
}