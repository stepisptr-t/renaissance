package org.renaissance.disruptor;

import com.lmax.disruptor.EventHandler;
import org.agrona.collections.Long2ObjectHashMap;

final class AnomalyDetectorHandler implements EventHandler<TelemetryEvent> {
    private static final double ANOMALY_THRESHOLD = 1.3;
    private static final int WINDOW_SIZE = 100;

    private final Long2ObjectHashMap<DataSourceState> dataSourceStates = new Long2ObjectHashMap<>();

    @Override
    public void onEvent(TelemetryEvent event, long sequence, boolean endOfBatch) {
        if (!event.isReady) return;

        long dataSourceId = event.dataSourceId;
        DataSourceState state = dataSourceStates.computeIfAbsent(dataSourceId, id -> new DataSourceState(WINDOW_SIZE));

        boolean anomaly = false;
        for (int i = 0; i < 6; i++) {
            double currentTorqueRms = state.torques[i].update(event.torques[i]);
            double torqueBaseline = state.torques[i].getBaseline();
            if (torqueBaseline > 0 && currentTorqueRms > torqueBaseline * ANOMALY_THRESHOLD) {
                anomaly = true;
            }

            double currentTempRms = state.temperatures[i].update(event.temperatures[i]);
            double tempBaseline = state.temperatures[i].getBaseline();
            if (tempBaseline > 0 && currentTempRms > tempBaseline * ANOMALY_THRESHOLD) {
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

    private static class RollingRms {
        private final int windowSize;
        private final double[] window;
        private double sumSq = 0;
        private int cursor = 0;
        private int samplesSeen = 0;
        private double baselineRms = -1;

        RollingRms(int windowSize) {
            this.windowSize = windowSize;
            this.window = new double[windowSize];
        }

        public double update(double newValue) {
            double oldValue = window[cursor];
            sumSq = sumSq - (oldValue * oldValue) + (newValue * newValue);
            window[cursor] = newValue;
            
            cursor = (cursor + 1) % windowSize;
            if (baselineRms == -1) {
                samplesSeen++;
            }

            double currentRms = Math.sqrt(sumSq / windowSize);

            if (baselineRms == -1 && samplesSeen >= windowSize) {
                baselineRms = currentRms;
            }

            return currentRms;
        }

        public double getBaseline() {
            return baselineRms;
        }
    }
}
