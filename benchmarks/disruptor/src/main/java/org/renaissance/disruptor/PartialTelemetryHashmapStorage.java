package org.renaissance.disruptor;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.Map;

public class PartialTelemetryHashmapStorage implements PartialTelemetryStorage {

    Map<Long, PartialTelemetry> storage = new Long2ObjectHashMap<>();

    @Override
    public void clear() {
        storage.clear();
    }

    @Override
    public PartialTelemetry readOrReset(long observationId) {
        return storage.computeIfAbsent(observationId, it -> new PartialTelemetry());
    }

    @Override
    public void writeBack(long observationId, PartialTelemetry pt) {
        storage.put(observationId, pt);
    }
}
