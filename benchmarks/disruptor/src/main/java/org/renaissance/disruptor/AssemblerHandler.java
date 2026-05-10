package org.renaissance.disruptor;

import com.lmax.disruptor.EventHandler;

final class AssemblerHandler implements EventHandler<TelemetryEvent> {
    private final PartialTelemetryStorage partialStorage;

    public AssemblerHandler(PartialTelemetryStorage partialStorage) {
        this.partialStorage = partialStorage;
    }

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        long key = event.observationId;
        PartialTelemetry pt = partialStorage.readOrReset(key);

        switch (event.type) {
            case DATA_SOURCE_ID:
                pt.dataSourceId = event.dataSourceId;
                pt.partsMask |= 1;
                break;
            case TORQUE:
                System.arraycopy(event.torques, 0, pt.torques, 0, 6);
                pt.partsMask |= 2;
                break;
            case TEMPERATURE:
                System.arraycopy(event.temperatures, 0, pt.temperatures, 0, 6);
                pt.partsMask |= 4;
                break;
        }

        if (pt.isComplete()) {
            event.dataSourceId = pt.dataSourceId;
            System.arraycopy(pt.torques, 0, event.torques, 0, 6);
            System.arraycopy(pt.temperatures, 0, event.temperatures, 0, 6);
            event.isReady = true;
            return;
        }
        event.isReady = false;
        partialStorage.writeBack(key, pt);
    }
}
