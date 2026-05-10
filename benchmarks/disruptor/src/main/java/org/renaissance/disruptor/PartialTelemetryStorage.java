package org.renaissance.disruptor;

public interface PartialTelemetryStorage {
    void clear();
    
    PartialTelemetry readOrReset(long observationId);

    void writeBack(long observationId, PartialTelemetry pt);
}
