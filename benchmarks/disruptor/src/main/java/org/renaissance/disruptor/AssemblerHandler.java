package org.renaissance.disruptor;

import com.lmax.disruptor.EventHandler;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Supplier;

final class AssemblerHandler implements EventHandler<TelemetryEvent> {
    // simplified hashmap because we know that every event will eventually be completed
    // in a production environment there would ba an embedded LRU linked list ordered by arrival time
    private final Long2ObjectHashMap<PartialTelemetry> partials = new Long2ObjectHashMap<>();
    private final ObjectPool<PartialTelemetry> pool;

    public AssemblerHandler(int maxPoolSize) {
        this.pool = new ObjectPool<>(maxPoolSize, PartialTelemetry::new);
    }

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        long key = event.observationId;
        PartialTelemetry pt = partials.computeIfAbsent(key, k -> pool.acquire());

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
            event.observationId = key;
            event.dataSourceId = pt.dataSourceId;
            System.arraycopy(pt.torques, 0, event.torques, 0, 6);
            System.arraycopy(pt.temperatures, 0, event.temperatures, 0, 6);
            event.isReady = true;

            partials.remove(key);
            pt.reset();
            pool.release(pt);
        } else {
            event.isReady = false;
        }
        
        if (endOfBatch) {
            pool.trim();
        }
    }

    private static class ObjectPool<T> {
        private final Deque<T> pool = new ArrayDeque<>();
        private final int maxSize;
        private final Supplier<T> factory;

        public ObjectPool(int maxSize, Supplier<T> factory) {
            this.maxSize = maxSize;
            this.factory = factory;
        }

        public T acquire() {
            return pool.isEmpty() ? factory.get() : pool.pop();
        }

        public void release(T obj) {
            pool.push(obj);
        }

        public void trim() {
            while (pool.size() > maxSize) {
                pool.pop();
            }
        }
    }

    private static final class PartialTelemetry {
        long observationId;
        long dataSourceId;
        final double[] torques = new double[6];
        final double[] temperatures = new double[6];
        int partsMask = 0;

        boolean isComplete() { return partsMask == 7; }
        void reset() { partsMask = 0; }
    }
}
