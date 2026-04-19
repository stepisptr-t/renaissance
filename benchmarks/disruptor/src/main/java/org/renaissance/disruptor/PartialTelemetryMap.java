package org.renaissance.disruptor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Uses a simple binary encoding of the PartialTelemetry objects into an offheap byte buffer, 
 * which is used to store the elements.
 * The size of the array is determined by the expected throughput and the desired lifetime.
 * It expects that the ObservationIds are generated in a linear fashion, 
 * which means that older observations have lower id and newer will overwrite the old ones 
 * once the array index computed from the ID wraps around.
 */
final class PartialTelemetryMap {

    private final ByteBuffer buffer;
    private final int mask;
    private final PartialTelemetry flyweight = new PartialTelemetry();

    public PartialTelemetryMap(int expectedThroughput, int lifetimeSeconds) {
        long expectedObservationsPerSec = expectedThroughput / PartialEventType.values().length;
        long requiredCapacity = expectedObservationsPerSec * lifetimeSeconds;

        int size = 1;
        while (size < requiredCapacity) {
            size <<= 1;
        }

        this.buffer = ByteBuffer.allocateDirect(size * PartialTelemetry.SIZE_IN_BYTES).order(ByteOrder.nativeOrder());
        this.mask = size - 1;

        clear();
    }

    public void clear() {
        buffer.clear();
        byte[] zeros = new byte[4096];
        while (buffer.remaining() > 0) {
            int length = Math.min(buffer.remaining(), zeros.length);
            buffer.put(zeros, 0, length);
        }
        buffer.clear();
    }

    /**
     * Once the observationId wraps around the size of the array, it overwrites old events,
     * whose lifetime had expired.
     * This is a deliberate choice in the tradeoff of the speed of access and lifecycle management
     * at the expense of memory usage.
     */
    public PartialTelemetry readOrReset(long observationId) {
        int index = (int) (observationId & mask);
        int offset = index * PartialTelemetry.SIZE_IN_BYTES;

        flyweight.readFrom(buffer, offset);

        if (flyweight.observationId != observationId) {
            flyweight.reset();
            flyweight.observationId = observationId;
        }
        return flyweight;
    }

    public void writeBack(long observationId, PartialTelemetry pt) {
        int index = (int) (observationId & mask);
        int offset = index * PartialTelemetry.SIZE_IN_BYTES;
        pt.writeTo(buffer, offset);
    }
}
