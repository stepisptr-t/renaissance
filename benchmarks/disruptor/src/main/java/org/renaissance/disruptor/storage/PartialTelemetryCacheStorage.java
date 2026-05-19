package org.renaissance.disruptor.storage;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;

public final class PartialTelemetryCacheStorage implements PartialTelemetryStorage {

    private final Cache<Long, PartialTelemetry> cache;

    public PartialTelemetryCacheStorage(int expectedLifetimeMillis) {
        cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMillis(expectedLifetimeMillis))
                .build();
        clear();
    }

    public void clear() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    public PartialTelemetry readOrReset(long observationId) {
        return cache.get(observationId, id -> new PartialTelemetry());
    }

    public void writeBack(long observationId, PartialTelemetry pt) {
        cache.put(observationId, pt);
    }
}
