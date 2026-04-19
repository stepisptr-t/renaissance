package org.renaissance.disruptor;

import java.nio.ByteBuffer;

final class PartialTelemetry {
    public static final int SIZE_IN_BYTES = 120; // 8+8+4+48+48 = 116 bytes -> 120 aligned to 8 bytes

    long observationId = -1;
    long dataSourceId;
    final double[] torques = new double[6];
    final double[] temperatures = new double[6];
    int partsMask = 0;

    boolean isComplete() { return partsMask == 7; }
    void reset() { partsMask = 0; observationId = 0; }

    void readFrom(ByteBuffer buffer, int offset) {
        this.observationId = buffer.getLong(offset);
        this.dataSourceId = buffer.getLong(offset + 8);
        this.partsMask = buffer.getInt(offset + 16);
        for (int i = 0; i < 6; i++) {
            this.torques[i] = buffer.getDouble(offset + 24 + i * 8);
        }
        for (int i = 0; i < 6; i++) {
            this.temperatures[i] = buffer.getDouble(offset + 72 + i * 8);
        }
    }

    void writeTo(ByteBuffer buffer, int offset) {
        buffer.putLong(offset, this.observationId);
        buffer.putLong(offset + 8, this.dataSourceId);
        buffer.putInt(offset + 16, this.partsMask);
        for (int i = 0; i < 6; i++) {
            buffer.putDouble(offset + 24 + i * 8, this.torques[i]);
        }
        for (int i = 0; i < 6; i++) {
            buffer.putDouble(offset + 72 + i * 8, this.temperatures[i]);
        }
    }
}
