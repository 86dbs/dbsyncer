/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.junit.Assert;
import org.junit.Test;

/**
 * 集群内部共享密钥校验。
 *
 * @author wuji
 * @version 1.0.0
 */
public class ClusterInternalAuthFilterTest {

    @Test
    public void testMatches() {
        Assert.assertTrue(ClusterInternalAuthFilter.matches("secret", "secret"));
        Assert.assertFalse(ClusterInternalAuthFilter.matches("secret", "Secret"));
        Assert.assertFalse(ClusterInternalAuthFilter.matches("secret", null));
        Assert.assertFalse(ClusterInternalAuthFilter.matches(null, "secret"));
        Assert.assertFalse(ClusterInternalAuthFilter.matches("ab", "abc"));
    }

    @Test
    public void testProtectedPath() {
        Assert.assertTrue(ClusterInternalAuthFilter.isProtectedPath("/cluster/internal/execute"));
        Assert.assertTrue(ClusterInternalAuthFilter.isProtectedPath("/cluster/internal/stop"));
        Assert.assertTrue(ClusterInternalAuthFilter.isProtectedPath("/cluster/metrics"));
        Assert.assertFalse(ClusterInternalAuthFilter.isProtectedPath("/cluster/ping"));
        Assert.assertFalse(ClusterInternalAuthFilter.isProtectedPath("/sso/consume"));
        Assert.assertFalse(ClusterInternalAuthFilter.isProtectedPath("/cluster/list"));
    }
}
