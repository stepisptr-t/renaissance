package org.renaissance.disruptor;

import com.lmax.disruptor.EventHandler;
import java.util.Set;

final class AnomalyPersistenceHandler implements EventHandler<TelemetryEvent> {
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
