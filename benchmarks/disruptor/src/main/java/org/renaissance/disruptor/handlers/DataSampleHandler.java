package org.renaissance.disruptor.handlers;

import com.lmax.disruptor.EventHandler;
import org.renaissance.disruptor.util.TelemetryEvent;
import org.renaissance.disruptor.storage.TelemetrySampleStorage;

import java.util.concurrent.atomic.AtomicLong;

public final class DataSampleHandler implements EventHandler<TelemetryEvent> {
    public static final int SAMPLE_FRQCY = 100;

    private final TelemetrySampleStorage store;
    private final AtomicLong count;

    public DataSampleHandler(TelemetrySampleStorage store, AtomicLong count) {
        this.store = store;
        this.count = count;
    }

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        if (!event.isReady) return;
        
        long c = count.getAndIncrement();
        if (c % SAMPLE_FRQCY == 0) {
            store.store(event);
        }
    }
}
