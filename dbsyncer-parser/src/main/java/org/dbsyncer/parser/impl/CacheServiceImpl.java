package org.dbsyncer.parser.impl;

import org.dbsyncer.parser.CacheService;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 22:39
 */
@Component
public class CacheServiceImpl implements CacheService {

    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    @Override
    public Object put(String key, Object value) {
        return cache.put(key, value);
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        return cache.putIfAbsent(key, value);
    }

    @Override
    public void remove(String key) {
        cache.remove(key);
    }

    @Override
    public Object get(String key) {
        return cache.get(key);
    }

    @Override
    public <T> T get(String key, Class<T> valueType) {
        return (T) cache.get(key);
    }

    @Override
    public Map<String, Object> getAll() {
        return Collections.unmodifiableMap(cache);
    }

}
