package org.renaissance.disruptor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class TelemetrySampleStorage {

    private final ByteBuffer buffer;
    private int currentOffset = 0;

    public TelemetrySampleStorage(int maxSamples) {
        this.buffer = ByteBuffer.allocateDirect(maxSamples * TelemetryEvent.DATA_BYTE_SIZE).order(ByteOrder.nativeOrder());
    }

    public boolean store(TelemetryEvent event) {
        if (currentOffset + TelemetryEvent.DATA_BYTE_SIZE > buffer.capacity()) {
            return false;
        }
        currentOffset += event.writeDataTo(buffer, currentOffset);
        return true;
    }

    public void reset() {
        this.currentOffset = 0;
    }

    public int sampleCount() {
        return currentOffset / TelemetryEvent.DATA_BYTE_SIZE;
    }
}
