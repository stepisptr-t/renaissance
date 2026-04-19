package org.renaissance.disruptor;

final class PartialTelemetryMap {

    private final PartialTelemetry[] partials;
    private final int mask;

    public PartialTelemetryMap(int expectedThroughput, int lifetimeSeconds) {
        long expectedObservationsPerSec = expectedThroughput / PartialEventType.values().length;
        long requiredCapacity = expectedObservationsPerSec * lifetimeSeconds;

        int size = 1;
        while (size < requiredCapacity) {
            size <<= 1;
        }
        this.partials = new PartialTelemetry[size];
        this.mask = size - 1;
        for (int i = 0; i < size; i++) {
            this.partials[i] = new PartialTelemetry();
        }
    }

    public void clear() {
        for (PartialTelemetry pt : partials) {
            pt.reset();
            pt.observationId = -1;
        }
    }

    /**
     * Once the observationId wraps around the size of the array, it overwrites old events,
     * whose lifetime had expired.
     * This is a deliberate choice in the tradeoff between speed of access and lifetime management
     * and memory usage (this wastes a lot of memory).
     */
    public PartialTelemetry getOrReset(long observationId) {
        int index = (int) (observationId & mask);
        PartialTelemetry pt = partials[index];

        if (pt.observationId != observationId) {
            pt.reset();
            pt.observationId = observationId;
        }
        return pt;
    }
}
