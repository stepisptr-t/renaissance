package org.renaissance.disruptor;

import com.lmax.disruptor.EventHandler;
import org.agrona.collections.Long2ObjectHashMap;

final class AnomalyDetectorHandler implements EventHandler<TelemetryEvent> {
    private static final double ANOMALY_THRESHOLD = 1.3;
    private static final double THRESHOLD_SQ = ANOMALY_THRESHOLD * ANOMALY_THRESHOLD;
    private static final int WINDOW_SIZE = 100;

    private final Long2ObjectHashMap<DataSourceState> dataSourceStates = new Long2ObjectHashMap<>();

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        if (!event.isReady) return;

        long dataSourceId = event.dataSourceId;
        DataSourceState state = dataSourceStates.computeIfAbsent(dataSourceId, id -> new DataSourceState(WINDOW_SIZE));

        boolean anomaly = false;
        for (int i = 0; i < 6; i++) {
            if (state.torques[i].updateAndCheck(event.torques[i])) {
                anomaly = true;
            }
            if (state.temperatures[i].updateAndCheck(event.temperatures[i])) {
                anomaly = true;
            }
        }

        event.isAnomaly = anomaly;
    }

    private static class DataSourceState {
        final RollingRms[] torques = new RollingRms[6];
        final RollingRms[] temperatures = new RollingRms[6];

        DataSourceState(int windowSize) {
            for (int i = 0; i < 6; i++) {
                torques[i] = new RollingRms(windowSize);
                temperatures[i] = new RollingRms(windowSize);
            }
        }
    }

    /**
     * Optimized RMS which calculates a rolling sum of squares.
     * 
     * We skip the square root and division by windowSize when comparing against the baseline because:
     * sqrt(sumSq_current / N) > sqrt(sumSq_baseline / N) * THRESHOLD
     * is mathematically equivalent to:
     * sumSq_current > sumSq_baseline * (THRESHOLD^2)
     */
    private static class RollingRms {
        private final int windowSize;
        private final double[] window;
        private double sumSq = 0;
        private int cursor = 0;
        private int samplesSeen = 0;
        private double baselineSumSqThreshold = -1;

        RollingRms(int windowSize) {
            this.windowSize = windowSize;
            this.window = new double[windowSize];
        }

        public boolean updateAndCheck(double newValue) {
            double oldValue = window[cursor];
            sumSq = sumSq - (oldValue * oldValue) + (newValue * newValue);
            window[cursor] = newValue;
            
            cursor++;
            if (cursor == windowSize) {
                cursor = 0;
            }
            
            if (baselineSumSqThreshold < 0) {
                samplesSeen++;
                if (samplesSeen >= windowSize) {
                    baselineSumSqThreshold = sumSq * THRESHOLD_SQ;
                }
                return false;
            }

            return baselineSumSqThreshold > 0 && sumSq > baselineSumSqThreshold;
        }
    }
}
