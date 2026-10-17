/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.model.WebSsoTicket;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-12 14:43
 */
public interface SsoTicketService {

    WebSsoTicket consume(String ticket);

    String sanitizeRedirect(String redirect);

    boolean isAllowedTarget(String target);

    String issue(String username, String roleCode, String target);

    String normalizeTargetBase(String target);
}
