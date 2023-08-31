package org.dbsyncer.manager.puller;

import org.dbsyncer.common.config.TableGroupBufferConfig;
import org.dbsyncer.common.event.ChangedEvent;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.dbsyncer.parser.model.WriterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public final class BufferActuatorRouter implements DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TableGroupBufferConfig tableGroupBufferConfig;

    @Resource
    private TableGroupBufferActuator tableGroupBufferActuator;

    @Resource
    private Parser parser;

    /**
     * 驱动缓存执行路由列表
     */
    private Map<String, Map<String, TableGroupBufferActuator>> router = new ConcurrentHashMap<>();

    public void execute(String metaId, String tableGroupId, ChangedEvent event) {
        if (router.containsKey(metaId) && router.get(metaId).containsKey(tableGroupId)) {
            router.get(metaId).get(tableGroupId).offer(new WriterRequest(tableGroupId, event));
            return;
        }
        parser.execute(tableGroupId, event);
    }

    public void bind(String metaId, String tableGroupId) {
        router.computeIfAbsent(metaId, k -> new ConcurrentHashMap<>());

        // TODO 暂定执行器上限，待替换为LRU模型
        if (router.get(metaId).size() >= tableGroupBufferConfig.getMaxBufferActuatorSize()) {
            return;
        }

        router.get(metaId).computeIfAbsent(tableGroupId, k -> {
            TableGroupBufferActuator newBufferActuator = null;
            try {
                newBufferActuator = (TableGroupBufferActuator) tableGroupBufferActuator.clone();
                newBufferActuator.setTableGroupId(tableGroupId);
                newBufferActuator.buildConfig();
            } catch (CloneNotSupportedException ex) {
                logger.error(ex.getMessage(), ex);
            }
            return newBufferActuator;
        });
    }

    public void unbind(String metaId) {
        if (router.containsKey(metaId)) {
            router.get(metaId).values().forEach(TableGroupBufferActuator::stop);
            router.remove(metaId);
        }
    }

    @Override
    public void destroy() {
        router.values().forEach(map -> map.values().forEach(TableGroupBufferActuator::stop));
        router.clear();
    }

    public AtomicLong getQueueSize() {
        AtomicLong total = new AtomicLong();
        router.values().forEach(map -> map.values().forEach(actuator -> total.addAndGet(actuator.getQueue().size())));
        return total;
    }

    public AtomicLong getQueueCapacity() {
        AtomicLong total = new AtomicLong();
        router.values().forEach(map -> map.values().forEach(actuator -> total.addAndGet(actuator.getQueueCapacity())));
        return total;
    }

    public Map<String, Map<String, TableGroupBufferActuator>> getRouter() {
        return Collections.unmodifiableMap(router);
    }

}