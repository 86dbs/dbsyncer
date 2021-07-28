package org.dbsyncer.web.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.web")
public class CacheConfiguration {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, CacheConfig> cache;

    @Bean
    public KeyGenerator cacheKeyGenerator() {
        return (target, method, params) -> {
            String className = method.getDeclaringClass().getSimpleName();
            String methodName = method.getName();
            String paramHash = String.valueOf(Arrays.toString(params).hashCode());
            String cacheKey = new StringJoiner("_").add(className).add(methodName).add(paramHash).toString();
            logger.debug("generate cache key : {}", cacheKey);
            return cacheKey;
        };
    }

    @Bean
    public Ticker ticker() {
        return Ticker.systemTicker();
    }

    @Bean
    public CacheManager cacheManager(Ticker ticker) {
        SimpleCacheManager manager = new SimpleCacheManager();
        if (cache != null) {
            List<CaffeineCache> caches = cache.entrySet()
                    .stream()
                    .map(entry -> buildCache(entry.getKey(), entry.getValue(), ticker))
                    .collect(Collectors.toList());
            manager.setCaches(caches);
        }
        return manager;
    }

    private CaffeineCache buildCache(String key, CacheConfig config, Ticker ticker) {
        logger.info("Cache key {} specified timeout of {} seconds, max of {}", key, config.getTimeout(), config.getMax());
        final Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder()
                .expireAfterWrite(config.getTimeout(), TimeUnit.SECONDS)
                .maximumSize(config.getMax())
                .ticker(ticker);
        return new CaffeineCache(key, caffeineBuilder.build());
    }

    static class CacheConfig {
        private Integer timeout;
        private Integer max = 200;

        public Integer getTimeout() {
            return timeout;
        }

        public void setTimeout(Integer timeout) {
            this.timeout = timeout;
        }

        public Integer getMax() {
            return max;
        }

        public void setMax(Integer max) {
            this.max = max;
        }
    }

    public void setCache(Map<String, CacheConfig> cache) {
        this.cache = cache;
    }

}