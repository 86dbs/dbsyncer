/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.cluster;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ClusterWriteProxyPaths} 白名单。
 *
 * @author wuji
 * @version 1.0.0
 */
public class ClusterWriteProxyPathsTest {

    @Test
    public void testTaskWritePathsProxied() {
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/database-sync/start"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/database-sync/add"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/validate-sync/stop"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/mapping/start"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/tableGroup/add"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/connector/add"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/connector/edit"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/connector/remove"));
        Assert.assertTrue(ClusterWriteProxyPaths.shouldProxy("POST", "/connector/copy"));
    }

    @Test
    public void testReadOrOtherNotProxied() {
        Assert.assertFalse(ClusterWriteProxyPaths.shouldProxy("GET", "/database-sync/start"));
        Assert.assertFalse(ClusterWriteProxyPaths.shouldProxy("POST", "/database-sync/search"));
        Assert.assertFalse(ClusterWriteProxyPaths.shouldProxy("POST", "/connector/test"));
        Assert.assertFalse(ClusterWriteProxyPaths.shouldProxy("POST", "/cluster/transfer"));
    }
}
