package org.renaissance.disruptor.storage;

public interface PartialTelemetryStorage {
    void clear();
    
    PartialTelemetry readOrReset(long observationId);

    void writeBack(long observationId, PartialTelemetry pt);
}
