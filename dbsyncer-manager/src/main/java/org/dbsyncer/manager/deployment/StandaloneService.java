/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.deployment;

import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.sdk.model.ClusterNode;
import org.dbsyncer.sdk.spi.ClusterService;

import java.util.Collections;
import java.util.List;

/**
 * 单机控制面：本机即执行者，调度方法空操作。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public final class StandaloneService implements ClusterService {

    public static final String NODE_ID = "standalone";

    public StandaloneService(MetaProfile metaProfile) {
        // metaProfile 保留构造参数以兼容既有注入
    }

    @Override
    public boolean isStandalone() {
        return true;
    }

    @Override
    public String getLocalNodeId() {
        return NODE_ID;
    }

    @Override
    public List<ClusterNode> listNodes() {
        return Collections.emptyList();
    }
}
