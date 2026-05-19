package org.renaissance.disruptor.handlers;

import com.lmax.disruptor.EventHandler;
import org.renaissance.disruptor.storage.PartialTelemetry;
import org.renaissance.disruptor.storage.PartialTelemetryStorage;
import org.renaissance.disruptor.util.TelemetryEvent;

public final class AssemblerHandler implements EventHandler<TelemetryEvent> {
    private final PartialTelemetryStorage partialStorage;

    public AssemblerHandler(PartialTelemetryStorage partialStorage) {
        this.partialStorage = partialStorage;
    }

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        long key = event.observationId;
        PartialTelemetry pt = partialStorage.readOrReset(key);

        pt.updateFrom(event);

        if (pt.isComplete()) {
            pt.copyTo(event);
            event.isReady = true;
            return;
        }
        event.isReady = false;
        partialStorage.writeBack(key, pt);
    }
}
