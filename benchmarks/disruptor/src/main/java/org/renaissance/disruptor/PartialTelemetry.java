package org.renaissance.disruptor;

final class PartialTelemetry {
    long observationId = -1;
    long dataSourceId;
    final double[] torques = new double[6];
    final double[] temperatures = new double[6];
    int partsMask = 0;

    boolean isComplete() { return partsMask == 7; }
    void reset() { partsMask = 0; }
}

