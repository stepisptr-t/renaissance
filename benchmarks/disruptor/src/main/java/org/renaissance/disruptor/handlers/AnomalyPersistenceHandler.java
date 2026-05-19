package org.renaissance.disruptor.handlers;

import com.lmax.disruptor.EventHandler;
import org.renaissance.disruptor.util.TelemetryEvent;

import java.util.Set;

public final class AnomalyPersistenceHandler implements EventHandler<TelemetryEvent> {
    private final Set<Long> detectedFailingDataSources;

    public AnomalyPersistenceHandler(Set<Long> detectedFailingDataSources) {
        this.detectedFailingDataSources = detectedFailingDataSources;
    }

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        if (!event.isReady) return;
        
        if (event.isAnomaly) {
            detectedFailingDataSources.add(event.dataSourceId);
        }
    }
}
