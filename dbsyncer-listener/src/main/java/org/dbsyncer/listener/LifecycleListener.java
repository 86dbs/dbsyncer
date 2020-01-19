package org.dbsyncer.listener;

/**
 * <h3>监听驱动停止</h3>
 * <ol type="1">
 * <li><dt>onClose</dt></li>
 * <dd>当驱动停止时会触发该事件</dd>
 * </ol>
 *
 * @ClassName: Listener
 * @author: AE86
 * @date: 2018年7月30日 上午10:58:54
 */
public interface LifecycleListener extends Runnable {

    /**
     * 驱动停止
     *
     * @Title: onClose
     * @Description: 监听驱动停止事件
     * @return: void
     */
    void onClose();

}
