package org.renaissance.disruptor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class TelemetrySampleStorage {

    private final ByteBuffer buffer;
    private int currentOffset = 0;

    public TelemetrySampleStorage(int maxSamples) {
        this.buffer = ByteBuffer.allocateDirect(maxSamples * TelemetryEvent.DATA_BYTE_SIZE).order(ByteOrder.nativeOrder());
    }

    public void store(TelemetryEvent event) {
        if (currentOffset + TelemetryEvent.DATA_BYTE_SIZE > buffer.capacity()) {
            throw new ArrayIndexOutOfBoundsException(currentOffset);
        }
        currentOffset += event.writeDataTo(buffer, currentOffset);
    }

    public Stream<TelemetryEvent> stream() {
        return IntStream.range(0, sampleCount())
                .mapToObj(i -> {
                    TelemetryEvent event = new TelemetryEvent();
                    event.readDataFrom(buffer, i * TelemetryEvent.DATA_BYTE_SIZE);
                    return event;
                });
    }

    public void reset() {
        this.currentOffset = 0;
    }

    public int sampleCount() {
        return currentOffset / TelemetryEvent.DATA_BYTE_SIZE;
    }
}
