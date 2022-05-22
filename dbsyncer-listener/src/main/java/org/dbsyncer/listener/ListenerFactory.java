package org.dbsyncer.listener;

import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.listener.enums.LogExtractorEnum;
import org.dbsyncer.listener.enums.TimingExtractorEnum;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ListenerFactory implements Listener {

    private Map<ListenerTypeEnum, ExtractorMapper> map = new LinkedHashMap<>();

    @PostConstruct
    private void init() {
        map.putIfAbsent(ListenerTypeEnum.LOG, (connectorType) -> LogExtractorEnum.getExtractor(connectorType));
        map.putIfAbsent(ListenerTypeEnum.TIMING, (connectorType) -> TimingExtractorEnum.getExtractor(connectorType));
    }

    @Override
    public <T> T getExtractor(ListenerTypeEnum listenerTypeEnum, String connectorType, Class<T> valueType) throws IllegalAccessException, InstantiationException {
        ExtractorMapper mapper = map.get(listenerTypeEnum);
        if (null == mapper) {
            throw new ListenerException(String.format("Unsupported type \"%s\" for extractor \"%s\".", listenerTypeEnum, connectorType));
        }

        Class<T> clazz = (Class<T>) mapper.getExtractor(connectorType);
        return clazz.newInstance();
    }

    interface ExtractorMapper {
        Class getExtractor(String connectorType);
    }

}